import argparse
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice

STAGES = (
    ("0", "counterfactual"),
    ("2", "harness"),
    ("3", "attribution"),
    ("4", "sufficiency"),
    ("5", "horizon"),
    ("6", "monotonicity"),
    ("7", "decomposition"),
    ("8", "data-value"),
    ("9", "release"),
)
QUEUE = "agent-queue"
QUEUE_ORDER = (
    "attribution",
    "data_value",
    "monotonicity",
    "sufficiency",
    "horizon",
    "decomposition",
)
WORKING = ("RUNNING", "TERMINATING")
VERDICT = re.compile(r"^(OK|PARTIAL|FAILED)\b:?\s*(.*)$")
IGNORED = frozenset({"minutes"})
ACTIVE = ("QUEUED", "PENDING", "BLOCKED", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY")
SHORT = 8
TEXT = 110


@dataclass(frozen=True)
class Run:
    run_id: int
    started: datetime
    params: dict[str, str]
    life_cycle: str
    result: str | None
    logs: str | None = None


@dataclass(frozen=True)
class Line:
    stage: str
    job: str
    experiment: str
    state: str
    run_id: int
    started: datetime
    runs: int
    text: str


def job_name(job: str, target: str) -> str:
    return f"pitlake-{job}-{target}"


def verdict(logs: str | None) -> tuple[str, str] | None:
    for line in reversed((logs or "").splitlines()):
        found = VERDICT.match(line.strip())
        if found:
            return found.group(1), found.group(2)
    return None


def experiment(params: dict[str, str], known: frozenset[str] | None = None) -> str:
    def shown(name: str, value: str) -> str:
        if name.endswith("backtest") and len(value) > SHORT:
            value = value[:SHORT]
        return f"{name}={value}"

    kept = sorted(
        (k, v)
        for k, v in params.items()
        if k not in IGNORED and v and (known is None or k in known)
    )
    return " ".join(shown(k, v) for k, v in kept) or "defaults"


def state(run: Run) -> tuple[str, str]:
    if run.life_cycle in ACTIVE:
        return run.life_cycle, ""
    if run.result != "SUCCESS":
        return run.result or run.life_cycle, ""
    found = verdict(run.logs)
    return found if found else ("SUCCESS", "")


def latest(
    stage: str, job: str, runs: Iterable[Run], known: frozenset[str] | None = None
) -> list[Line]:
    grouped: dict[str, list[Run]] = {}
    for run in runs:
        grouped.setdefault(experiment(run.params, known), []).append(run)
    lines = []
    for key, mine in grouped.items():
        last = max(mine, key=lambda r: (r.life_cycle in WORKING, r.started))
        word, text = state(last)
        lines.append(Line(stage, job, key, word, last.run_id, last.started, len(mine), text))
    return sorted(lines, key=lambda line: line.started, reverse=True)


def queue_rank(key: str) -> int:
    return QUEUE_ORDER.index(key) if key in QUEUE_ORDER else len(QUEUE_ORDER)


def clip(text: str) -> str:
    return text if len(text) <= TEXT else text[: TEXT - 3] + "..."


def render(lines: Iterable[Line]) -> list[str]:
    found = []
    for line in lines:
        head = (
            f"{line.stage:>2} {line.job:<15} {line.state:<10} run {line.run_id} "
            f"{line.started:%m-%d %H:%M} ({line.runs} run{'s' if line.runs != 1 else ''})"
        )
        found.append(head)
        found.append(f"     {line.experiment}")
        if line.text:
            found.append(f"     {clip(line.text)}")
    return found


def queue_lines(run_id: int, started: datetime, tasks: Iterable[tuple[str, str, str]]) -> list[str]:
    found = [f"queue run {run_id} started {started:%m-%d %H:%M}"]
    found += [f"  {key:<15} {life}{f' {result}' if result else ''}" for key, life, result in tasks]
    return found


def _params(run) -> dict[str, str]:
    return {
        p.name: (p.value if p.value is not None else p.default) or ""
        for p in run.job_parameters or []
    }


def _started(run) -> datetime:
    return datetime.fromtimestamp((run.start_time or 0) / 1000, UTC)


def _run(w, run) -> Run:
    life = run.state.life_cycle_state.value if run.state.life_cycle_state else ""
    result = run.state.result_state.value if run.state.result_state else None
    logs = None
    if result == "SUCCESS" and run.tasks:
        logs = w.jobs.get_run_output(run.tasks[0].run_id).logs
    return Run(run.run_id, _started(run), _params(run), life, result, logs)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target")
    parser.add_argument("--runs", type=int, default=20, help="Recent runs read per job")
    args = parser.parse_args(argv)

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    ids = {j.settings.name: j.job_id for j in w.jobs.list()}
    lines: list[Line] = []
    for stage, job in STAGES:
        job_id = ids.get(job_name(job, args.target))
        if job_id is None:
            continue
        known = frozenset(p.name for p in w.jobs.get(job_id).settings.parameters or [])
        recent = islice(w.jobs.list_runs(job_id=job_id, expand_tasks=True), args.runs)
        runs = [_run(w, r) for r in recent]
        lines += latest(stage, job, runs, known)
    for text in render(sorted(lines, key=lambda line: (line.stage, -line.started.timestamp()))):
        print(text)
    queue = ids.get(job_name(QUEUE, args.target))
    if queue is not None:
        for run in islice(w.jobs.list_runs(job_id=queue), 1):
            run = w.jobs.get_run(run.run_id)
            ordered = sorted(run.tasks or [], key=lambda t: queue_rank(t.task_key))
            tasks = [
                (
                    t.task_key,
                    t.state.life_cycle_state.value if t.state.life_cycle_state else "",
                    t.state.result_state.value if t.state.result_state else "",
                )
                for t in ordered
            ]
            print()
            for text in queue_lines(run.run_id, _started(run), tasks):
                print(text)


if __name__ == "__main__":
    main()

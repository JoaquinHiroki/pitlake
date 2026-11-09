import argparse
import hashlib
import json
import re
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime

from pitlake.ablation import apply, plan, recompute
from pitlake.backtest import Params, decision_times, fingerprint
from pitlake.bronze import parse_optional_date, sql_string
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.harness import (
    AGENT_REACH,
    DECIDERS,
    HARNESS_REACH,
    MAX_TOKENS,
    STRATEGY_DECIDER,
    STRATEGY_REACH,
    TEMPERATURE,
    Complete,
    Sample,
    Summary,
    View,
    agent_config,
    agent_tools,
    canonical,
    in_reach,
    run_agent,
    strategy_config,
    strategy_sample,
    summarize,
)
from pitlake.information import information_set, parse_item_id, sessions
from pitlake.jobs.information_set import (
    SHORT,
    Loaded,
    decisions_query,
    group_decisions,
    load,
)
from pitlake.jobs.research import package_version
from pitlake.tables import (
    DECISION_POINTS,
    DECISION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
)


class DecisionsUnstable(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decision points failed: {'; '.join(failures[:10])}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", required=True, help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision day (06:00 UTC); default the last")
    parser.add_argument("--withhold", default="", help="Item IDs withheld from the world")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10")
    parser.add_argument("--rounds", default="2")
    parser.add_argument("--min-agreement", default="0.8")
    parser.add_argument("--workers", default="2")
    parser.add_argument("--replay", default="", help="A harness run ID to decide again")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    for name in ("backtest", "replay"):
        value = getattr(args, name).strip()
        if value:
            validate_segment(value)
        setattr(args, name, value)
    args.at = parse_optional_date(args.at)
    named = [parse_item_id(i) for i in re.split(r"[\s;]+", args.withhold) if i]
    args.withhold = tuple(dict.fromkeys(named))
    args.deciders = tuple(dict.fromkeys(d for d in re.split(r"[\s,;]+", args.deciders) if d))
    unknown = sorted(set(args.deciders) - set(DECIDERS))
    if unknown or not args.deciders:
        parser.error(f"--deciders must name {' and/or '.join(DECIDERS)}, not {unknown}")
    args.samples, args.rounds, args.workers = (
        int(v.strip() or 0) for v in (args.samples, args.rounds, args.workers)
    )
    if args.samples < 1 or args.rounds < 1 or args.workers < 1:
        parser.error("--samples, --rounds and --workers must be at least 1")
    args.min_agreement = float(args.min_agreement.strip() or 0)
    if not 0 <= args.min_agreement <= 1:
        parser.error("--min-agreement is a share between 0 and 1")
    return args


@dataclass(frozen=True)
class Spec:
    decider: str
    record: dict
    at: date | None
    withheld: tuple[str, ...]
    samples: int
    rounds: int
    replay_of: str | None = None
    expected: str | None = None
    expected_config: str | None = None


def instant(record: dict, at: date | None) -> tuple[datetime, list[datetime]]:
    start, end = record["period_start"], record["period_end"]
    day = at or end
    if not start <= day <= end:
        raise ValueError(f"{day} is outside the backtest's period {start} to {end}")
    times = decision_times(start, day)
    return times[-1], times


def world(loaded: Loaded, withheld: tuple[str, ...]) -> list[tuple]:
    if not withheld:
        return loaded.rows
    removal = plan(loaded.rows, withheld, sessions(loaded.items))
    return apply(loaded.rows, removal, recompute(loaded.bars, removal, HARNESS_REACH))


def endpoint_caller(endpoint: str) -> Complete:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()

    def complete(messages: list[dict], tools: list[dict] | None) -> dict:
        body = {"messages": messages, "max_tokens": MAX_TOKENS, "temperature": TEMPERATURE}
        if tools:
            body["tools"] = tools
        found = client.api_client.do(
            "POST", f"/serving-endpoints/{endpoint}/invocations", body=body
        )
        choices = found.get("choices") or []
        if not choices or not choices[0].get("message"):
            raise RuntimeError(f"{endpoint} returned no answer")
        return choices[0]["message"]

    return complete


def timed(decide) -> Sample:
    started = time.perf_counter()
    try:
        sample = decide()
    except Exception as error:
        sample = Sample(None, "", (), 0, f"{type(error).__name__}: {error}")
    return replace(sample, seconds=time.perf_counter() - started)


def draw(decide, rounds: int, samples: int, workers: int) -> list[list[Sample]]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        drawn = list(pool.map(lambda _: timed(decide), range(rounds * samples)))
    return [drawn[r * samples : (r + 1) * samples] for r in range(rounds)]


def point_id(config_hash: str, at: datetime, versions: dict, withheld: tuple[str, ...]) -> str:
    return fingerprint([config_hash, at.isoformat(), sorted(versions.items()), sorted(withheld)])


@dataclass(frozen=True)
class Decided:
    spec: Spec
    point_id: str
    config: dict
    config_hash: str
    at: datetime
    versions: dict
    set_hash: str
    world_rows: int
    rounds: list[list[Sample]]
    summary: Summary
    verified: bool | None

    def reproduced(self) -> bool | None:
        if self.spec.replay_of is None:
            return None
        same = self.config_hash == self.spec.expected_config
        return same and self.summary.decision == self.spec.expected

    def passed(self, minimum: float) -> bool:
        return (
            self.summary.reproducible(minimum)
            and self.verified is not False
            and self.reproduced() is not False
        )


def decide(
    spec: Spec,
    loaded: Loaded,
    complete: Complete | None,
    workers: int,
    endpoint: str = "",
    minimum: float = 0.8,
) -> Decided:
    at, times = instant(spec.record, spec.at)
    rows = world(loaded, spec.withheld)
    verified = None
    if spec.decider == STRATEGY_DECIDER:
        params = Params.from_json(spec.record["params"])
        config = strategy_config(params)
        mine = in_reach(rows, STRATEGY_REACH)
        rounds = draw(lambda: strategy_sample(mine, times, params), spec.rounds, spec.samples, 1)
        if not spec.withheld and times[-1].date() == spec.record["period_end"]:
            verified = all(s.input_hash == spec.record["input_hash"] for r in rounds for s in r)
    else:
        if complete is None:
            raise RuntimeError("the agent needs a chat endpoint")
        config = agent_config(endpoint)
        view = View(in_reach(rows, AGENT_REACH), at)
        rounds = draw(
            lambda: run_agent(complete, agent_tools(view), at), spec.rounds, spec.samples, workers
        )
        mine = in_reach(rows, AGENT_REACH)
    config_hash = fingerprint(config)
    return Decided(
        spec,
        point_id(config_hash, at, loaded.versions, spec.withheld),
        config,
        config_hash,
        at,
        dict(loaded.versions),
        information_set(loaded.items, at, spec.withheld).hash,
        len(mine),
        rounds,
        summarize(rounds, minimum),
        verified,
    )


def digest_of(text: str | None) -> str | None:
    return None if text is None else hashlib.sha256(text.encode()).hexdigest()


def point_row(args, found: Decided, at: datetime) -> tuple:
    record, summary = found.spec.record, found.summary
    seconds = [s.seconds for r in found.rounds for s in r]
    return (
        found.point_id,
        args.run_id,
        at,
        found.spec.decider,
        canonical(found.config),
        found.config_hash,
        record["backtest_id"],
        record["research_run_id"],
        found.at,
        found.versions,
        list(found.spec.withheld),
        found.set_hash,
        found.world_rows,
        found.spec.samples,
        found.spec.rounds,
        summary.decision,
        list(summary.rounds),
        summary.agreement,
        summary.parts,
        summary.distinct,
        summary.invalid,
        args.min_agreement,
        found.passed(args.min_agreement),
        found.verified,
        found.spec.replay_of,
        found.reproduced(),
        statistics.median(seconds) if seconds else 0.0,
        args.code_commit,
        package_version(),
    )


def sample_rows(args, found: Decided, at: datetime) -> list[tuple]:
    return [
        (
            found.point_id,
            args.run_id,
            at,
            found.spec.decider,
            r,
            k,
            s.decision,
            digest_of(s.decision),
            s.input_hash,
            json.dumps([list(c) for c in s.calls]),
            s.model_calls,
            s.seconds,
            s.error,
            s.answer,
        )
        for r, found_round in enumerate(found.rounds)
        for k, s in enumerate(found_round)
    ]


def describe(found: Decided, minimum: float) -> str:
    summary = found.summary
    total = sum(len(r) for r in found.rounds)
    agreeing = round(summary.agreement * total)
    stable = sum(1 for r in summary.rounds if r == summary.decision)
    withheld = " ".join(found.spec.withheld) or "nothing"
    settled = ", ".join(
        f"{k} {v}" if isinstance(v, str) else k for k, v in sorted(summary.settled.items())
    )
    unsettled = ", ".join(
        f"{k} {summary.parts[k]:.2f}" for k in sorted(summary.parts) if k not in summary.settled
    )
    seconds = statistics.median([s.seconds for r in found.rounds for s in r])
    calls = statistics.median([s.model_calls for r in found.rounds for s in r])
    lines = [
        f"{found.spec.decider} at {found.at:%Y-%m-%d %H:%M} UTC, withheld {withheld} "
        f"(world {found.set_hash[:SHORT]}, {found.world_rows:,} rows): settled "
        f"{settled or 'nothing'} ({len(summary.settled)} of {len(summary.parts)} parts, each "
        f"agreed by at least {minimum:.2f} of samples) in {stable} of {len(summary.rounds)} "
        f"rounds; unsettled {unsettled or 'nothing'}; whole answers agree in {agreeing} of "
        f"{total} samples ({summary.agreement:.2f}); {summary.distinct} distinct, "
        f"{summary.invalid} invalid; median {seconds:.1f} s and {calls:g} model calls"
    ]
    if summary.decision is not None and len(summary.decision) < 400:
        lines.append(f"  decision {summary.decision}")
    if found.verified is not None:
        state = "equal" if found.verified else "NOT equal"
        lines.append(f"  inputs from the pure-Python as-of are {state} to the recorded hash")
    if found.spec.replay_of is not None:
        state = "IDENTICAL" if found.reproduced() else "DIFFERENT"
        lines.append(f"  replay of {found.spec.replay_of}: {state}")
    lines += [f"  invalid: {e}" for e in error_lines(found)]
    return "\n".join(lines)


def error_lines(found: Decided) -> list[str]:
    seen: dict[str, int] = {}
    for r in found.rounds:
        for s in r:
            if s.error:
                seen[s.error[:200]] = seen.get(s.error[:200], 0) + 1
    return [f"{n} x {e}" for e, n in sorted(seen.items(), key=lambda kv: -kv[1])[:5]]


def replay_query(catalog: str, run_id: str) -> str:
    return (
        "SELECT p.decider, p.backtest_id, p.decision_at, p.withheld, p.samples, p.rounds, "
        "p.decision, p.config_hash, p.point_id "
        f"FROM {table_fqn(catalog, 'control', DECISION_POINTS)} p "
        f"WHERE p.run_id = {sql_string(run_id)} AND p.replay_of IS NULL ORDER BY p.created_at"
    )


def specs(spark, args) -> list[Spec]:
    if args.replay:
        found = []
        for p in spark.sql(replay_query(args.catalog, args.replay)).collect():
            record = spark.sql(decisions_query(args.catalog, p["backtest_id"])).collect()[0]
            found.append(
                Spec(
                    p["decider"],
                    record.asDict(),
                    p["decision_at"].date(),
                    tuple(p["withheld"]),
                    p["samples"],
                    p["rounds"],
                    p["point_id"],
                    p["decision"],
                    p["config_hash"],
                )
            )
        if not found:
            raise RuntimeError(f"no decision points recorded by run {args.replay}")
        return found
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    return [
        Spec(d, record, args.at, args.withhold, args.samples, args.rounds) for d in args.deciders
    ]


def decide_all(spark, args, at: datetime) -> tuple[list[tuple], list[tuple], list[str]]:
    complete = endpoint_caller(args.endpoint)
    points, samples, failures, cache = [], [], [], {}
    for spec in specs(spark, args):
        key = next(iter(group_decisions([spec.record])))
        if key not in cache:
            cache[key] = load(spark, args.catalog, key, HARNESS_REACH)
        found = decide(spec, cache[key], complete, args.workers, args.endpoint, args.min_agreement)
        points.append(point_row(args, found, at))
        samples += sample_rows(args, found, at)
        passed = found.passed(args.min_agreement)
        print(f"{'REPRODUCIBLE' if passed else 'UNSTABLE'}: {describe(found, args.min_agreement)}")
        if not passed:
            failures.append(f"{spec.decider} {' '.join(spec.withheld) or 'base world'}")
    return points, samples, failures


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)

    points, samples, failures = decide_all(spark, args, at)
    spark.createDataFrame(samples, schema=DECISION_SAMPLES_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", DECISION_SAMPLES)
    )
    spark.createDataFrame(points, schema=DECISION_POINTS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", DECISION_POINTS)
    )
    if failures:
        raise DecisionsUnstable(failures)
    print(f"OK: {len(points)} decision points in {args.catalog}, each reproducible")


if __name__ == "__main__":
    main()

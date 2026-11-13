import argparse
import re
import time
import uuid
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from pitlake.attribution import calls_of, inputs_hash
from pitlake.backtest import fingerprint
from pitlake.bronze import parse_optional_date
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.decomposition import (
    ACROSS,
    AFTER,
    BEFORE,
    BLIND,
    CONDITIONS,
    INFERRED,
    LEVEL,
    MEMORISED,
    PARTS,
    PROBE_QUESTION,
    PROBE_SYSTEM,
    PROBES,
    RECALL,
    STRADDLES,
    TOLERANCE,
    TOOLED,
    TOOLS_OVER_MEMORY,
    Estimate,
    Question,
    Recalled,
    Scored,
    condition_config,
    decompose,
    era,
    estimate,
    probe_prompts,
    questions,
    recall,
    recall_error,
    run_untooled,
    text_of,
)
from pitlake.harness import (
    AGENT_DECIDER,
    AGENT_REACH,
    Complete,
    Sample,
    View,
    agent_tools,
    mode,
    run_agent,
)
from pitlake.information import feature_rows
from pitlake.jobs.attribute import Writer, keys_in
from pitlake.jobs.decide import draw, endpoint_caller, instant, samples_of
from pitlake.jobs.information_set import decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.jobs.suffice import Late
from pitlake.jobs.supply import closes_query, returns_text
from pitlake.monotonicity import DAYS, Outcome, outcome, world_quality
from pitlake.tables import (
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
    DECOMPOSITION_SCORES,
    DECOMPOSITION_SCORES_SCHEMA,
    DECOMPOSITION_TESTS,
    DECOMPOSITION_TESTS_SCHEMA,
    DECOMPOSITIONS,
    DECOMPOSITIONS_SCHEMA,
    RECALL_PROBES,
    RECALL_PROBES_SCHEMA,
)

DECOMPOSED = "DECOMPOSED"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
SCHEMAS = {
    RECALL_PROBES: RECALL_PROBES_SCHEMA,
    DECOMPOSITION_TESTS: DECOMPOSITION_TESTS_SCHEMA,
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    DECOMPOSITION_SCORES: DECOMPOSITION_SCORES_SCHEMA,
    DECOMPOSITIONS: DECOMPOSITIONS_SCHEMA,
}
LABELS = {
    "prior": "prior (blind)",
    "recall_gain": "recall gain",
    "tool_gain": "tool gain",
    "apparent": "apparent (tooled)",
}


class DecompositionFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} instants failed decomposition: {'; '.join(failures)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", required=True, help="Chat endpoint the agent decides with")
    parser.add_argument("--cutoff", default="", help="The endpoint's training cutoff (a date)")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days; default the 1st of every month")
    parser.add_argument("--samples", default="10", help="Agent samples per condition and instant")
    parser.add_argument("--days", default=str(DAYS), help="Days of outcome after the instant")
    parser.add_argument("--tolerance", default=str(TOLERANCE), help="Recall error that counts")
    parser.add_argument("--minutes", default="100", help="Stop starting decisions after this long")
    parser.add_argument("--workers", default="2")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.endpoint = args.endpoint.strip()
    if not args.endpoint:
        parser.error("--endpoint names the chat endpoint to decompose")
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.backtest = args.backtest.strip()
    if args.backtest:
        validate_segment(args.backtest)
    try:
        args.cutoff = parse_optional_date(args.cutoff)
    except ValueError as error:
        parser.error(f"--cutoff: {error}")
    if args.cutoff is None:
        parser.error("--cutoff is the endpoint's training cutoff, a date such as 2024-09-01")
    days = [parse_optional_date(d) for d in re.split(r"[\s,;]+", args.at) if d]
    args.at = tuple(sorted(set(days)))
    args.samples, args.workers, args.days = (
        int(v.strip() or 0) for v in (args.samples, args.workers, args.days)
    )
    if min(args.samples, args.workers, args.days) < 1:
        parser.error("--samples, --workers and --days must be at least 1")
    args.tolerance = float(args.tolerance.strip() or 0)
    if args.tolerance <= 0:
        parser.error("--tolerance must be above 0")
    args.minutes = float(args.minutes.strip() or 0)
    return args


def monthly(start: date, end: date) -> tuple[date, ...]:
    found, day = [], date(start.year, start.month, 1)
    while day <= end:
        if day >= start:
            found.append(day)
        day = date(day.year + day.month // 12, day.month % 12 + 1, 1)
    return tuple(found)


@dataclass(frozen=True)
class Decided:
    test_id: str
    key: str
    condition: str
    at: datetime | None
    decisions: tuple[str | None, ...]
    valid: bool
    input_hash: str
    samples: int = 0
    invalid: int = 0
    errors: int = 0
    seconds: float = 0.0


def decision_key(condition: str, endpoint: str, at: datetime, versions: dict, samples: int) -> str:
    return fingerprint(
        [
            condition,
            fingerprint(condition_config(condition, endpoint)),
            None if condition == BLIND else at.isoformat(),
            sorted(versions.items()) if condition == TOOLED else None,
            samples,
        ]
    )


def decision_row(args, test: Decided, versions: dict, at: datetime) -> tuple:
    return (
        test.test_id,
        test.key,
        args.run_id,
        at,
        args.endpoint,
        test.condition,
        fingerprint(condition_config(test.condition, args.endpoint)),
        test.at,
        dict(versions) if test.condition == TOOLED else {},
        test.samples,
        test.invalid,
        test.errors,
        test.valid,
        list(test.decisions),
        mode(test.decisions),
        test.input_hash,
        test.seconds,
        args.code_commit,
        package_version(),
    )


class Decider:
    def __init__(
        self,
        complete: Complete,
        rows: list[tuple],
        versions: dict,
        args,
        writer: Writer,
        deadline: float,
        at: datetime,
        stored: dict[str, Decided],
    ):
        self.complete, self.rows, self.versions = complete, rows, versions
        self.args, self.writer, self.deadline, self.at = args, writer, deadline, at
        self.tests = dict(stored)
        self.earlier = len(self.tests)
        self.decided = 0
        self.failed = 0

    def key(self, condition: str, at: datetime) -> str:
        return decision_key(condition, self.args.endpoint, at, self.versions, self.args.samples)

    def __call__(self, condition: str, at: datetime) -> Decided:
        key = self.key(condition, at)
        if key in self.tests:
            return self.tests[key]
        if time.monotonic() > self.deadline:
            raise Late
        return self.test(condition, at, key)

    def test(self, condition: str, at: datetime, key: str) -> Decided:
        started = time.perf_counter()
        if condition == TOOLED:
            tools = agent_tools(View(self.rows, at))

            def decide() -> Sample:
                return run_agent(self.complete, tools, at)
        else:
            tools = None

            def decide() -> Sample:
                return run_untooled(self.complete, condition, at)

        samples = draw(decide, 1, self.args.samples, self.args.workers)[0]
        errors = sum(1 for s in samples if not s.valid and not s.model_calls)
        seen = inputs_hash(tools, calls_of(samples)) if tools else fingerprint([])
        test = Decided(
            uuid.uuid4().hex,
            key,
            condition,
            None if condition == BLIND else at,
            tuple(s.decision for s in samples),
            not errors,
            seen,
            len(samples),
            sum(1 for s in samples if not s.valid),
            errors,
            time.perf_counter() - started,
        )
        self.writer.add(
            DECOMPOSITION_TESTS, [decision_row(self.args, test, self.versions, self.at)]
        )
        self.writer.add(
            DECISION_SAMPLES,
            samples_of(
                self.args.run_id, test.test_id, AGENT_DECIDER, [samples], self.at
            ),  # fmt: skip
        )
        self.writer.flush(force=False)
        self.decided += 1
        if test.valid:
            self.tests[key] = test
        else:
            self.failed += 1
        return test


def probe_key(endpoint: str, symbol: str, session: date) -> str:
    return fingerprint(
        [endpoint, PROBE_SYSTEM, PROBE_QUESTION, symbol, session.isoformat(), PROBES]
    )


@dataclass(frozen=True)
class Answered:
    question: Question
    answers: tuple[str | None, ...]
    valid: bool
    seconds: float

    @property
    def estimate(self) -> float | None:
        return estimate(self.answers)


def ask(complete: Complete, question: Question) -> Answered:
    started = time.perf_counter()
    answers, valid = [], True
    for _ in range(PROBES):
        try:
            reply = complete(probe_prompts(question.symbol, question.session), None)
            answers.append(text_of(reply.get("content"))[:200])
        except Exception:
            answers.append(None)
            valid = False
    return Answered(question, tuple(answers), valid, time.perf_counter() - started)


def probe_row(args, found: Answered, at: datetime) -> tuple:
    q = found.question
    return (
        probe_key(args.endpoint, q.symbol, q.session),
        args.run_id,
        at,
        args.endpoint,
        q.symbol,
        q.session,
        q.actual,
        list(found.answers),
        found.estimate,
        recall_error(found.estimate, q.actual),
        found.valid,
        found.seconds,
        args.code_commit,
        package_version(),
    )


def probe(
    complete: Complete,
    asked: Iterable[Question],
    stored: dict[str, float | None],
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> tuple[dict[tuple[str, date], float | None], int]:
    unique = {q.key: q for q in asked}
    found = {}
    left = []
    for key, q in sorted(unique.items()):
        stored_key = probe_key(args.endpoint, q.symbol, q.session)
        if stored_key in stored:
            found[key] = stored[stored_key]
        else:
            left.append(q)
    if not left or time.monotonic() > deadline:
        return found, len(left)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        answered = list(pool.map(lambda q: ask(complete, q), left))
    writer.add(RECALL_PROBES, [probe_row(args, a, at) for a in answered])
    writer.flush()
    missing = 0
    for a in answered:
        if a.valid:
            found[a.question.key] = a.estimate
        else:
            missing += 1
    return found, missing


@dataclass(frozen=True)
class Instant:
    day: date
    at: datetime
    era: str
    outcome: Outcome | None = None
    unknown: str | None = None
    tests: tuple[Decided, ...] = ()
    failed: int = 0

    @property
    def by_condition(self) -> dict[str, Decided]:
        return {t.condition: t for t in self.tests if t.valid}

    @property
    def state(self) -> str:
        if self.unknown:
            return FAILED
        return DECOMPOSED if set(self.by_condition) == set(CONDITIONS) else PARTIAL

    def quality(self, condition: str) -> float:
        return world_quality(
            AGENT_DECIDER, self.by_condition[condition].decisions, self.outcome.returns
        )

    def scored(self) -> Scored:
        return Scored(self.at, self.era, *(self.quality(c) for c in CONDITIONS))


def describe(found: Instant) -> str:
    head = f"{found.at:%Y-%m-%d %H:%M} UTC, {found.era} the cutoff"
    if found.unknown:
        return f"{head}\n  problem: {found.unknown}"
    head += f"; {returns_text(found.outcome)}"
    if found.state != DECOMPOSED:
        text = (
            f"{head}; stopped with {len(found.by_condition)} of {len(CONDITIONS)} conditions "
            "decided"
        )
        if found.failed:
            text += (
                f"; the endpoint failed in {found.failed} of them, which the next run decides again"
            )
        return text
    shown = ", ".join(f"{c} {found.quality(c):+.4f}" for c in CONDITIONS)
    invalid = sum(t.invalid for t in found.tests)
    text = f"{head}\n  quality: {shown}"
    if invalid:
        text += f" ({invalid} samples gave no valid answer and score 0)"
    return text


def stored_tests_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT test_id, test_key, condition, decision_at, decisions, valid, input_hash, samples, "
        "invalid, errors, seconds "
        f"FROM {table_fqn(catalog, 'control', DECOMPOSITION_TESTS)} "
        f"WHERE test_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_probes_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT probe_key, estimate FROM "
        f"{table_fqn(catalog, 'control', RECALL_PROBES)} "
        f"WHERE probe_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_test(row) -> Decided:
    return Decided(
        row["test_id"],
        row["test_key"],
        row["condition"],
        row["decision_at"],
        tuple(row["decisions"]),
        row["valid"],
        row["input_hash"],
        row["samples"],
        row["invalid"],
        row["errors"],
        row["seconds"],
    )


def log_error(value: float) -> str:
    return "no answer" if value == float("inf") else f"{value:.3f}"


def price(value: float | None) -> str:
    return "no answer" if value is None else f"{value:,.2f}"


def recall_lines(endpoint: str, found: list[Recalled], tolerance: float) -> list[str]:
    lines = [
        f"RECALL: {endpoint} asked with no tools for the close public at each instant and window "
        f"end ({PROBES} answers, the median kept); recalled when the median over symbols of "
        f"|ln(answer / close)| is at most {tolerance:g}",
    ]
    for r in found:
        closes = ", ".join(
            f"{s} {price(r.estimates[s])} for {r.actuals[s]:,.2f}" for s in sorted(r.actuals)
        )
        state = "recalled" if r.recalled(tolerance) else "not recalled"
        lines.append(f"  {r.at:%Y-%m-%d} {r.era:<6} {log_error(r.error):>9} {state:<12}  {closes}")
    for where in (BEFORE, AFTER):
        mine = [r for r in found if r.era == where]
        if mine:
            hits = sum(1 for r in mine if r.recalled(tolerance))
            lines.append(f"  {where} the cutoff: {hits} of {len(mine)} times recalled")
    return lines


def estimate_text(found: Estimate | None) -> str:
    if found is None or found.value is None:
        return "n/a"
    if not found.interval:
        return f"{found.value:+.4f} (no interval)"
    return f"{found.value:+.4f} [{found.low:+.4f}, {found.high:+.4f}]"


def decomposition_lines(args, estimates: list[Estimate], scored: list[Scored], straddling: int):
    by_key = {(e.component, e.era): e for e in estimates}
    before = sum(1 for s in scored if s.era == BEFORE)
    after = sum(1 for s in scored if s.era == AFTER)
    lines = [
        f"DECOMPOSITION: {args.endpoint}, cutoff {args.cutoff}, {before} instants before it and "
        f"{after} after ({straddling} straddling it left out); quality is the active return of the "
        f"tilt over the {args.days} days after each instant, with {LEVEL * 100:g}% bootstrap "
        "intervals over instants",
        f"  {'':<18} {'before the cutoff':<34} after the cutoff",
    ]
    for name in PARTS:
        lines.append(
            f"  {LABELS[name]:<18} {estimate_text(by_key.get((name, BEFORE))):<34} "
            f"{estimate_text(by_key.get((name, AFTER)))}"
        )
    lines += [
        f"  {MEMORISED}: {estimate_text(by_key[MEMORISED, ACROSS])}, the recall gain before the "
        "cutoff minus after it",
        f"  {INFERRED}: {estimate_text(by_key[INFERRED, AFTER])}, the tool gain after the cutoff",
        f"  tools over memory: {estimate_text(by_key[TOOLS_OVER_MEMORY, ACROSS])}, the tool gain "
        "before the cutoff minus after it",
    ]
    return lines


def score_rows(args, record: dict, found: Iterable[Instant], errors: dict, at: datetime):
    rows = []
    for i in found:
        tests = i.by_condition
        s = i.scored()
        rows.append(
            (
                args.run_id,
                at,
                args.endpoint,
                record["backtest_id"],
                args.cutoff,
                i.at,
                i.era,
                args.days,
                i.outcome.end,
                dict(i.outcome.returns),
                s.blind,
                s.recall,
                s.tooled,
                tests[BLIND].test_id,
                tests[RECALL].test_id,
                tests[TOOLED].test_id,
                errors.get(i.at),
                args.code_commit,
                package_version(),
            )
        )
    return rows


def decomposition_rows(args, record: dict, estimates: Iterable[Estimate], at: datetime):
    return [
        (
            args.run_id,
            at,
            args.endpoint,
            record["backtest_id"],
            args.cutoff,
            args.days,
            args.samples,
            e.component,
            e.era,
            e.instants,
            e.value,
            e.low,
            e.high,
            LEVEL,
            args.code_commit,
            package_version(),
        )
        for e in estimates
    ]


@dataclass(frozen=True)
class Result:
    instants: list[Instant]
    left_out: list[tuple[date, str]]
    straddling: tuple[date, ...]
    recalled: list[Recalled]
    unasked: int
    estimates: list[Estimate]


def instants_of(record: dict, args, closes: list[tuple]):
    explicit = bool(args.at)
    days = args.at or monthly(record["period_start"], record["period_end"])
    if not days:
        raise ValueError(
            f"the backtest's period {record['period_start']} to {record['period_end']} has no "
            "first of a month; name the instants with at"
        )
    found, left_out, straddling = [], [], []
    for day in days:
        at = instant(record, day)[0]
        where = era(at, args.days, args.cutoff)
        if where == STRADDLES:
            straddling.append(day)
            continue
        try:
            scored = outcome(closes, at, args.days)
        except ValueError as error:
            if not explicit:
                left_out.append((day, str(error)))
                continue
            found.append(Instant(day, at, where, unknown=str(error)))
            continue
        found.append(Instant(day, at, where, scored))
    return found, left_out, tuple(straddling)


def decompose_instants(
    record: dict,
    rows: list[tuple],
    versions: dict,
    closes: list[tuple],
    complete: Complete,
    stored_tests: dict[str, Decided],
    stored_probes: dict[str, float | None],
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Result:
    found, left_out, straddling = instants_of(record, args, closes)
    times = [t for i in found if i.outcome for t in (i.at, i.at + timedelta(days=args.days))]
    asked = questions(closes, times)
    estimates, unasked = probe(
        complete, [q for qs in asked.values() for q in qs], stored_probes, args, writer,
        deadline, at,
    )  # fmt: skip
    recalled = recall(
        {t: [q for q in qs if q.key in estimates] for t, qs in asked.items()},
        estimates,
        args.cutoff,
    )
    decider = Decider(complete, rows, versions, args, writer, deadline, at, stored_tests)
    done = []
    for i in found:
        if i.unknown:
            done.append(i)
            continue
        tests, failed = [], 0
        for condition in CONDITIONS:
            try:
                test = decider(condition, i.at)
            except Late:
                break
            tests.append(test)
            failed += not test.valid
        done.append(Instant(i.day, i.at, i.era, i.outcome, None, tuple(tests), failed))
    writer.flush()
    errors = {r.at: r.error for r in recalled}
    complete_set = [i for i in done if i.state == DECOMPOSED]
    result = []
    if len(complete_set) == len(done) and not unasked:
        result = decompose(
            [i.scored() for i in complete_set], seed=f"{args.endpoint}/{args.cutoff}"
        )
        writer.add(DECOMPOSITION_SCORES, score_rows(args, record, complete_set, errors, at))
        writer.add(DECOMPOSITIONS, decomposition_rows(args, record, result, at))
        writer.flush()
    return Result(done, left_out, straddling, recalled, unasked, result)


def decompose_all(spark, args, at: datetime) -> tuple[dict, Result]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    loaded = load(spark, args.catalog, next(iter(group_decisions([record]))), AGENT_REACH)
    closes = feature_rows(spark.sql(closes_query(args.catalog)))
    found, *_ = instants_of(record, args, closes)
    keys = [
        decision_key(c, args.endpoint, i.at, loaded.versions, args.samples)
        for i in found
        for c in CONDITIONS
    ]
    stored_tests = {}
    for row in spark.sql(stored_tests_query(args.catalog, keys or [""])).collect():
        stored_tests[row["test_key"]] = stored_test(row)
    times = [t for i in found if i.outcome for t in (i.at, i.at + timedelta(days=args.days))]
    probe_keys = [
        probe_key(args.endpoint, q.symbol, q.session)
        for qs in questions(closes, times).values()
        for q in qs
    ]
    stored_probes = {
        row["probe_key"]: row["estimate"]
        for row in spark.sql(stored_probes_query(args.catalog, probe_keys or [""])).collect()
    }
    writer = Writer(spark, args.catalog, SCHEMAS)
    result = decompose_instants(
        record,
        loaded.rows,
        loaded.versions,
        closes,
        endpoint_caller(args.endpoint),
        stored_tests,
        stored_probes,
        args,
        writer,
        time.monotonic() + args.minutes * 60,
        at,
    )
    return record, result


def report(args, result: Result) -> list[str]:
    lines = recall_lines(args.endpoint, result.recalled, args.tolerance)
    if result.unasked:
        lines.append(
            f"  {result.unasked} closes could not be asked about; the next run asks them again"
        )
    straddles = f"its {args.days}-day window straddles the cutoff"
    left = [*result.left_out, *((day, straddles) for day in result.straddling)]
    for day, reason in sorted(left):
        lines.append(f"LEFT OUT: {day}: {reason}")
    for i in result.instants:
        lines.append(f"{i.state}: {describe(i)}")
    if result.estimates:
        scored = [i.scored() for i in result.instants]
        lines += decomposition_lines(args, result.estimates, scored, len(result.straddling))
    return lines


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)

    _, result = decompose_all(spark, args, datetime.now(UTC))
    for line in report(args, result):
        print(line)
    failures = [f"{i.at:%Y-%m-%d}: {i.unknown}" for i in result.instants if i.state == FAILED]
    if failures:
        raise DecompositionFailed(failures)
    left = [i for i in result.instants if i.state == PARTIAL]
    if left or result.unasked:
        print(
            f"{PARTIAL}: {len(left)} of {len(result.instants)} instants still have conditions to "
            "decide; run the job again with the same parameters to continue"
        )
        return
    by_key = {(e.component, e.era): e for e in result.estimates}
    print(
        f"OK: {len(result.instants)} instants decomposed in {args.catalog}; {MEMORISED} "
        f"{estimate_text(by_key[MEMORISED, ACROSS])}, {INFERRED} "
        f"{estimate_text(by_key[INFERRED, AFTER])}"
    )


if __name__ == "__main__":
    main()

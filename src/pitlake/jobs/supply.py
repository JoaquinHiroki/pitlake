import argparse
import re
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from pitlake.attribution import EVALUATED, SAMPLED, calls_of, inputs_hash
from pitlake.bronze import parse_optional_date, sql_string
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.harness import (
    AGENT_DECIDER,
    AGENT_REACH,
    DECIDERS,
    HARNESS_REACH,
    STRATEGY_DECIDER,
    STRATEGY_REACH,
    SYMBOLS,
    Complete,
    Sample,
    View,
    agent_tools,
    mode,
    run_agent,
    strategy_sample,
)
from pitlake.information import feature_rows, information_set
from pitlake.jobs.attribute import Point, Writer, keys_in, make_point
from pitlake.jobs.decide import draw, endpoint_caller, samples_of
from pitlake.jobs.information_set import SHORT, Loaded, decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.jobs.suffice import Builder, Late, ablation_of
from pitlake.monotonicity import (
    CLOSE,
    DAYS,
    DRAWS,
    LEVELS,
    Outcome,
    Verdict,
    kept_at,
    ladder,
    ladder_text,
    middle,
    outcome,
    percent,
    shuffled,
    verdict,
    world_quality,
)
from pitlake.sufficiency import by_kind, kept_hash
from pitlake.tables import (
    ABLATION_CHANGES,
    ABLATION_CHANGES_SCHEMA,
    ABLATIONS,
    ABLATIONS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
    MONOTONICITY_CURVES,
    MONOTONICITY_CURVES_SCHEMA,
    MONOTONICITY_PAIRS,
    MONOTONICITY_PAIRS_SCHEMA,
    MONOTONICITY_TESTS,
    MONOTONICITY_TESTS_SCHEMA,
)
from pitlake.tables import FEATURES as FEATURES_TABLE

MEASURED = "MEASURED"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
MONOTONIC = "MONOTONIC"
NON_MONOTONIC = "NON-MONOTONIC"
VERIFY_ALL = "all"
VERIFY_NONE = "none"
VERIFY = (VERIFY_ALL, VERIFY_NONE)
SCHEMAS = {
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    MONOTONICITY_TESTS: MONOTONICITY_TESTS_SCHEMA,
    ABLATION_CHANGES: ABLATION_CHANGES_SCHEMA,
    ABLATIONS: ABLATIONS_SCHEMA,
    MONOTONICITY_CURVES: MONOTONICITY_CURVES_SCHEMA,
}
SHOWN = 5


class MonotonicityFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decisions failed monotonicity: {'; '.join(failures)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", default="", help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10", help="Agent samples per world")
    parser.add_argument("--levels", default=ladder_text(LEVELS), help="Shares of items supplied")
    parser.add_argument("--draws", default=str(DRAWS), help="Random orders per decision")
    parser.add_argument("--days", default=str(DAYS), help="Days of outcome after the instant")
    parser.add_argument("--verify", default=VERIFY_ALL, choices=VERIFY)
    parser.add_argument("--minutes", default="100", help="Stop starting worlds after this long")
    parser.add_argument("--workers", default="2")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.backtest = args.backtest.strip()
    if args.backtest:
        validate_segment(args.backtest)
    days = [parse_optional_date(d) for d in re.split(r"[\s,;]+", args.at) if d]
    args.at = tuple(dict.fromkeys(days)) or (None,)
    args.deciders = tuple(dict.fromkeys(d for d in re.split(r"[\s,;]+", args.deciders) if d))
    unknown = sorted(set(args.deciders) - set(DECIDERS))
    if unknown or not args.deciders:
        parser.error(f"--deciders must name {' and/or '.join(DECIDERS)}, not {unknown}")
    if AGENT_DECIDER in args.deciders and not args.endpoint.strip():
        parser.error("the agent needs --endpoint")
    args.samples, args.workers, args.draws, args.days = (
        int(v.strip() or 0) for v in (args.samples, args.workers, args.draws, args.days)
    )
    if min(args.samples, args.workers, args.draws, args.days) < 1:
        parser.error("--samples, --workers, --draws and --days must be at least 1")
    try:
        args.levels = ladder(float(f) for f in re.split(r"[\s,;]+", args.levels) if f)
    except ValueError as error:
        parser.error(f"--levels: {error}")
    args.min_agreement = 1.0
    args.minutes = float(args.minutes.strip() or 0)
    return args


@dataclass(frozen=True)
class Tried:
    test_id: str
    kept: int
    decisions: tuple[str | None, ...]
    valid: bool
    status: str
    input_hash: str
    rebuilt: bool | None = None


@dataclass(frozen=True)
class Probed:
    fraction: float
    draw: int
    kept: frozenset[str]
    tried: Tried
    samples: list[Sample]
    errors: int
    removed: int
    recomputed: int
    seconds: float


def world_row(args, point: Point, test: Probed, returns: dict, at: datetime) -> tuple:
    tried = test.tried
    return (
        tried.test_id,
        point.key,
        args.run_id,
        at,
        test.fraction,
        test.draw,
        kept_hash(test.kept),
        len(test.kept),
        by_kind(test.kept),
        tried.status,
        tried.valid,
        len(test.samples),
        sum(1 for s in test.samples if not s.valid),
        test.errors,
        list(tried.decisions),
        mode(tried.decisions),
        args.days,
        world_quality(point.decider, tried.decisions, returns),
        tried.input_hash,
        test.removed,
        test.recomputed,
        tried.rebuilt,
        test.seconds,
        args.code_commit,
        package_version(),
    )


class Supplier:
    def __init__(
        self,
        spark,
        point: Point,
        builder: Builder,
        complete: Complete | None,
        args,
        writer: Writer,
        deadline: float,
        at: datetime,
        tried: dict[str, Tried],
        returns: dict[str, float],
    ):
        self.spark, self.point, self.builder, self.complete = spark, point, builder, complete
        self.args, self.writer, self.deadline, self.at = args, writer, deadline, at
        self.returns = returns
        self.tried = dict(tried)
        self.seen: dict[str, Tried] = {}
        self.earlier = len(self.tried)
        self.broken = sum(1 for t in self.tried.values() if t.rebuilt is False)

    def __call__(self, fraction: float, drawn: int, kept: frozenset[str]) -> Tried:
        key = kept_hash(kept)
        found = self.tried.get(key) or self.seen.get(key)
        if found is None:
            if time.monotonic() > self.deadline:
                raise Late
            found = self.test(kept, fraction, drawn)
            self.seen[key] = found
        return found

    def test(self, kept: frozenset[str], fraction: float, drawn: int) -> Tried:
        started = time.perf_counter()
        point, builder = self.point, self.builder
        world = builder.build(self.spark, kept)
        if point.decider == STRATEGY_DECIDER:
            rows = builder.rows(world, STRATEGY_REACH)
            samples = [strategy_sample(rows, point.times, point.params)]
            errors, seen, status = 0, samples[0].input_hash, EVALUATED
        else:
            tools = agent_tools(View(builder.rows(world, AGENT_REACH), point.at))
            samples = draw(
                lambda: run_agent(self.complete, tools, point.at),
                1,
                point.samples,
                self.args.workers,
            )[0]
            errors = sum(1 for s in samples if not s.valid and not s.model_calls)
            seen = inputs_hash(tools, calls_of(samples))
            status = SAMPLED
        rebuilt = builder.rebuilt(world) if self.args.verify == VERIFY_ALL else None
        tried = Tried(
            uuid.uuid4().hex,
            len(kept),
            tuple(s.decision for s in samples),
            not errors,
            status,
            seen,
            rebuilt,
        )
        test = Probed(
            fraction,
            drawn,
            kept,
            tried,
            samples,
            errors,
            len(world.removal.removed),
            len(world.added),
            time.perf_counter() - started,
        )
        row = world_row(self.args, point, test, self.returns, self.at)
        self.writer.add(MONOTONICITY_TESTS, [row])
        if status == SAMPLED:
            self.writer.add(
                DECISION_SAMPLES,
                samples_of(self.args.run_id, tried.test_id, point.decider, [samples], self.at),
            )
        self.writer.flush(force=False)
        if rebuilt is False:
            self.broken += 1
        if tried.valid:
            self.tried[kept_hash(kept)] = tried
        return tried

    @property
    def sampled(self) -> int:
        return sum(1 for t in self.tried.values() if t.status == SAMPLED)

    @property
    def errors(self) -> int:
        return sum(1 for t in self.seen.values() if not t.valid)


@dataclass(frozen=True)
class Link:
    draw: int
    fraction: float
    test_id: str
    kept: int
    quality: float
    rebuilt: bool | None


def curve_rows(
    args,
    point: Point,
    found: Outcome,
    links: Iterable[Link],
    ablation_id: str | None,
    coherent: bool | None,
    at: datetime,
) -> list[tuple]:
    return [
        (
            point.key,
            args.run_id,
            at,
            point.decider,
            point.at,
            ladder_text(args.levels),
            args.draws,
            args.days,
            found.end,
            dict(found.returns),
            link.fraction,
            link.draw,
            link.kept,
            link.test_id,
            link.quality,
            link.rebuilt,
            ablation_id,
            coherent,
            args.code_commit,
            package_version(),
        )
        for link in links
    ]


def stored_tests_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, test_id, kept_hash, kept_count, decisions, valid, status, input_hash, "
        f"rebuilt FROM {table_fqn(catalog, 'control', MONOTONICITY_TESTS)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_curves_query(catalog: str, keys: Iterable[str], args) -> str:
    return (
        "SELECT point_key, run_id, decision_at, outcome_end, returns, fraction, draw, kept_count, "
        "test_id, quality, rebuilt, ablation_id, coherent "
        f"FROM {table_fqn(catalog, 'control', MONOTONICITY_CURVES)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND ladder = {sql_string(ladder_text(args.levels))} "
        f"AND draws = {int(args.draws)} AND outcome_days = {int(args.days)} ORDER BY created_at"
    )


def closes_query(catalog: str) -> str:
    symbols = ", ".join(sql_string(s) for s in SYMBOLS)
    return (
        "SELECT entity, feature, event_time, knowable_at, value, source_table, source_ref "
        f"FROM {table_fqn(catalog, 'gold', FEATURES_TABLE)} "
        f"WHERE feature = {sql_string(CLOSE)} AND entity IN ({symbols})"
    )


def stored_tried(row) -> Tried:
    return Tried(
        row["test_id"],
        row["kept_count"],
        tuple(row["decisions"]),
        row["valid"],
        row["status"],
        row["input_hash"],
        row["rebuilt"],
    )


@dataclass(frozen=True)
class Stored:
    outcome: Outcome
    links: tuple[Link, ...]
    ablation_id: str | None
    coherent: bool | None


def stored_curves(rows: Iterable) -> dict[str, Stored]:
    by_point: dict[str, list] = {}
    for row in rows:
        by_point.setdefault(row["point_key"], []).append(row)
    found = {}
    for key, every in by_point.items():
        last = every[-1]["run_id"]
        mine = [r for r in every if r["run_id"] == last]
        links = tuple(
            sorted(
                (
                    Link(
                        r["draw"],
                        r["fraction"],
                        r["test_id"],
                        r["kept_count"],
                        r["quality"],
                        r["rebuilt"],
                    )  # fmt: skip
                    for r in mine
                ),
                key=lambda link: (link.draw, link.fraction),
            )
        )
        first = mine[0]
        scored = Outcome(dict(first["returns"]), first["decision_at"], first["outcome_end"])
        found[key] = Stored(scored, links, first["ablation_id"], first["coherent"])
    return found


@dataclass(frozen=True)
class Progress:
    point: Point
    candidates: int
    tests: int
    earlier: int
    sampled: int
    broken: int
    outcome: Outcome | None = None
    unknown: str | None = None
    links: tuple[Link, ...] = ()
    ablation_id: str | None = None
    coherent: bool | None = None
    errors: int = 0

    @property
    def problems(self) -> list[str]:
        found = []
        if self.unknown:
            found.append(self.unknown)
        if self.broken:
            found.append(f"{self.broken} worlds differ from the full rebuild")
        if self.coherent is False:
            found.append(f"the ablation {self.ablation_id} is not coherent")
        return found

    @property
    def state(self) -> str:
        if self.problems:
            return FAILED
        return MEASURED if self.links else PARTIAL

    def chains(self) -> list[dict[float, float]]:
        found: dict[int, dict[float, float]] = {}
        for link in self.links:
            found.setdefault(link.draw, {})[link.fraction] = link.quality
        return [found[d] for d in sorted(found)]


def returns_text(found: Outcome) -> str:
    moves = ", ".join(f"{s} {r * 100:+.2f}%" for s, r in sorted(found.returns.items()))
    return f"outcome to {found.end:%Y-%m-%d %H:%M} UTC: {moves}"


def describe(progress: Progress, levels: Iterable[float]) -> str:
    point = progress.point
    head = f"{point.label} (point {point.key[:SHORT]}): {progress.candidates:,} items knowable"
    if progress.unknown:
        return f"{head}\n  problem: {progress.unknown}"
    head += (
        f"; {progress.tests:,} worlds decided ({progress.earlier:,} from earlier runs, "
        f"{progress.sampled:,} sampled)"
    )
    if progress.outcome is not None:
        head += f"; {returns_text(progress.outcome)}"
    if not progress.links:
        lines = [f"{head}; stopped before every world was decided"]
        if progress.errors:
            lines[0] += (
                f"; the endpoint failed in {progress.errors:,} worlds, which the next run "
                "decides again"
            )
    else:
        lines = [head, "      share    items  quality: mean over draws, lowest, highest"]
        for fraction in levels:
            mine = [link for link in progress.links if link.fraction == fraction]
            if not mine:
                continue
            values = [link.quality for link in mine]
            lines.append(
                f"  {percent(fraction):>9} {mine[0].kept:>8,}  {sum(values) / len(values):+.4f} "
                f"{min(values):+.4f} {max(values):+.4f}"
            )
        coherent = {None: "not checked", True: "coherent", False: "INCOHERENT"}
        if progress.ablation_id:
            lines.append(
                f"  checked: ablation {progress.ablation_id} {coherent[progress.coherent]}"
            )
    lines += [f"  problem: {p}" for p in progress.problems if p != progress.unknown]
    return "\n".join(lines)


def verdicts(found: list[Progress], levels: tuple[float, ...]) -> list[Verdict]:
    result = []
    for decider in DECIDERS:
        mine = [p for p in found if p.point.decider == decider]
        if not mine or any(p.state != MEASURED for p in mine):
            continue
        chains = [c for p in mine for c in p.chains()]
        result.append(verdict(decider, len(mine), chains, levels))
    return result


def verdict_lines(found: Verdict, days: int) -> list[str]:
    pairs = len(found.pairs)
    lines = [
        f"QUALITY: {found.decider} over {found.decisions} decisions, {found.chains} chains; "
        f"quality is the active return of its tilt over the {days} days after each instant",
    ]
    for fraction, mean in found.means.items():
        lines.append(f"  {percent(fraction):>9}: mean {mean:+.4f}")
    regions = found.regions
    if regions:
        spans = ", ".join(f"{percent(a)} to {percent(b)}" for a, b in regions)
        lines.append(
            f"  {NON_MONOTONIC} at 0.05 (Holm over {pairs} pairs): quality falls within {spans}"
        )
        falls = sorted((p for p in found.pairs if p.fell), key=lambda p: (p.falls_adjusted, p.low))
        for p in falls[:SHOWN]:
            lines.append(
                f"    {percent(p.low)} to {percent(p.high)}: {p.change:+.4f}, p {p.falls:.4f}, "
                f"adjusted {p.falls_adjusted:.4f}"
            )
    else:
        smallest = min(found.pairs, key=lambda p: (p.falls, p.low))
        lines.append(
            f"  {MONOTONIC} at 0.05 (Holm over {pairs} pairs): no larger share gives a "
            f"significantly lower quality; the strongest fall is {percent(smallest.low)} to "
            f"{percent(smallest.high)}, {smallest.change:+.4f}, p {smallest.falls:.4f}, "
            f"adjusted {smallest.falls_adjusted:.4f}"
        )
    rises = [p for p in found.pairs if p.rose]
    lines.append(
        f"  stops helping at {percent(found.saturates)}: no larger share is significantly better "
        f"from there; "
        f"{len(rises)} of {pairs} pairs rise"
    )
    whole = next(p for p in found.pairs if p.low == found.levels[0] and p.high == found.levels[-1])
    lines.append(
        f"  {percent(whole.low)} to {percent(whole.high)}: {whole.change:+.4f}, "
        f"p rises {whole.rises:.4f}, p falls {whole.falls:.4f}"
    )
    return lines


def pair_rows(args, found: Verdict, at: datetime) -> list[tuple]:
    return [
        (
            args.run_id,
            at,
            found.decider,
            ladder_text(found.levels),
            args.draws,
            args.days,
            found.decisions,
            found.chains,
            p.low,
            p.high,
            p.before,
            p.after,
            p.change,
            p.falls,
            p.rises,
            p.falls_adjusted,
            p.rises_adjusted,
            p.fell,
            p.rose,
            args.code_commit,
            package_version(),
        )
        for p in found.pairs
    ]


def supply_point(
    spark,
    point: Point,
    loaded: Loaded,
    builder: Builder,
    tried: dict[str, Tried],
    stored: Stored | None,
    closes: list[tuple],
    complete: Complete | None,
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Progress:
    info = information_set(loaded.items, point.at)
    items = list(info.items.values())
    sampled = sum(1 for t in tried.values() if t.status == SAMPLED)
    broken = sum(1 for t in tried.values() if t.rebuilt is False)

    def progress(supplier: Supplier | None = None, **found) -> Progress:
        if supplier is None:
            return Progress(point, len(items), len(tried), len(tried), sampled, broken, **found)
        return Progress(
            point,
            len(items),
            len(supplier.tried),
            supplier.earlier,
            supplier.sampled,
            supplier.broken,
            errors=supplier.errors,
            **found,
        )

    if stored is not None:
        return progress(
            outcome=stored.outcome,
            links=stored.links,
            ablation_id=stored.ablation_id,
            coherent=stored.coherent,
        )
    try:
        scored = outcome(closes, point.at, args.days)
    except ValueError as error:
        return progress(unknown=str(error))
    supplier = Supplier(
        spark, point, builder, complete, args, writer, deadline, at, tried, scored.returns
    )
    decided = []
    try:
        for drawn in range(args.draws):
            order = shuffled(items, f"{info.hash}/{drawn}")
            for fraction in args.levels:
                kept = kept_at(order, fraction)
                decided.append((drawn, fraction, kept, supplier(fraction, drawn, kept)))
    except Late:
        return progress(supplier, outcome=scored)
    if time.monotonic() > deadline or not all(t.valid for *_, t in decided):
        return progress(supplier, outcome=scored)
    links = tuple(
        Link(
            drawn,
            fraction,
            t.test_id,
            t.kept,
            world_quality(point.decider, t.decisions, scored.returns),
            t.rebuilt,
        )
        for drawn, fraction, _, t in decided
    )
    ablation_id, coherent, checked = None, None, middle(args.levels)
    if checked is not None:
        kept = next(k for d, f, k, _ in decided if d == 0 and f == checked)
        withheld = tuple(i for i in builder.known if i not in kept)
        ablation_id, coherent = ablation_of(spark, point, loaded, withheld, args, writer, at)
    writer.add(
        MONOTONICITY_CURVES, curve_rows(args, point, scored, links, ablation_id, coherent, at)
    )
    writer.flush()
    return progress(
        supplier, outcome=scored, links=links, ablation_id=ablation_id, coherent=coherent
    )


def supply_all(spark, args, at: datetime) -> list[Progress]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    loaded = load(spark, args.catalog, next(iter(group_decisions([record]))), HARNESS_REACH)
    points = [
        make_point(d, record, day, loaded.versions, args) for day in args.at for d in args.deciders
    ]
    keys = [p.key for p in points]
    tried: dict[str, dict[str, Tried]] = {}
    for row in spark.sql(stored_tests_query(args.catalog, keys)).collect():
        tried.setdefault(row["point_key"], {})[row["kept_hash"]] = stored_tried(row)
    curves = stored_curves(spark.sql(stored_curves_query(args.catalog, keys, args)).collect())
    closes = feature_rows(spark.sql(closes_query(args.catalog)))
    complete = endpoint_caller(args.endpoint) if AGENT_DECIDER in args.deciders else None
    builder, writer = Builder(loaded), Writer(spark, args.catalog, SCHEMAS)
    deadline = time.monotonic() + args.minutes * 60
    found = []
    for point in points:
        progress = supply_point(
            spark,
            point,
            loaded,
            builder,
            tried.get(point.key, {}),
            curves.get(point.key),
            closes,
            complete,
            args,
            writer,
            deadline,
            at,
        )
        writer.flush()
        print(f"{progress.state}: {describe(progress, args.levels)}")
        found.append(progress)
    return found


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)

    at = datetime.now(UTC)
    found = supply_all(spark, args, at)
    measured = verdicts(found, args.levels)
    writer = Writer(spark, args.catalog, {MONOTONICITY_PAIRS: MONOTONICITY_PAIRS_SCHEMA})
    for result in measured:
        for line in verdict_lines(result, args.days):
            print(line)
        writer.add(MONOTONICITY_PAIRS, pair_rows(args, result, at))
    writer.flush()
    failures = [f"{p.point.label}: {'; '.join(p.problems)}" for p in found if p.state == FAILED]
    if failures:
        raise MonotonicityFailed(failures)
    left = [p for p in found if p.state == PARTIAL]
    if left:
        print(
            f"{PARTIAL}: {len(left)} of {len(found)} decisions still have worlds to decide; run "
            "the job again with the same parameters to continue"
        )
        return
    states = "; ".join(
        f"{v.decider} {MONOTONIC if v.monotonic else NON_MONOTONIC}" for v in measured
    )
    print(f"OK: {len(found)} decisions in {args.catalog} have a quality curve; {states}")


if __name__ == "__main__":
    main()

import argparse
import json
import re
import time
import uuid
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace

from pitlake.ablation import COHERENT, Lineage, Plan, apply, differences, rebuild, recompute_each
from pitlake.attribution import (
    EVALUATED,
    SAMPLED,
    SCREENED,
    Base,
    Effect,
    agent_effect,
    calls_of,
    inputs_hash,
    null_passed,
    strategy_base,
    strategy_effect,
    unchanged,
)
from pitlake.bronze import parse_optional_date
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.harness import (
    AGENT_DECIDER,
    AGENT_REACH,
    DECIDERS,
    HARNESS_REACH,
    STRATEGY_DECIDER,
    STRATEGY_REACH,
    Complete,
    Sample,
    View,
    agent_tools,
    canonical,
    in_reach,
    run_agent,
    strategy_sample,
    summarize,
)
from pitlake.information import Item, Reach, information_set, sessions
from pitlake.jobs.ablation import ablate, ablation_row, change_rows
from pitlake.jobs.attribute import (
    Point,
    Writer,
    base_of,
    keys_in,
    make_point,
    point_row,
    stored_base,
    stored_points_query,
)
from pitlake.jobs.decide import draw, endpoint_caller, samples_of, timed
from pitlake.jobs.information_set import (
    REACH,
    SHORT,
    Loaded,
    decisions_query,
    group_decisions,
    load,
)
from pitlake.jobs.research import package_version
from pitlake.sufficiency import (
    VERIFY,
    VERIFY_ALL,
    VERIFY_FINAL,
    by_kind,
    hopeless,
    kept_hash,
    order,
    outline,
    reduce,
)
from pitlake.tables import (
    ABLATION_CHANGES,
    ABLATION_CHANGES_SCHEMA,
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
    SUFFICIENCY_TESTS,
    SUFFICIENCY_TESTS_SCHEMA,
    SUFFICIENT_SETS,
    SUFFICIENT_SETS_SCHEMA,
)

SUFFICIENT = "SUFFICIENT"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
SCHEMAS = {
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    ATTRIBUTION_POINTS: ATTRIBUTION_POINTS_SCHEMA,
    SUFFICIENCY_TESTS: SUFFICIENCY_TESTS_SCHEMA,
    ABLATION_CHANGES: ABLATION_CHANGES_SCHEMA,
    ABLATIONS: ABLATIONS_SCHEMA,
    SUFFICIENT_SETS: SUFFICIENT_SETS_SCHEMA,
}


class SufficiencyFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decisions failed sufficiency: {'; '.join(failures)}")


class Late(Exception):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", default="", help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10", help="Agent samples per world tried")
    parser.add_argument("--min-agreement", default="0.8")
    parser.add_argument("--verify", default=VERIFY_FINAL, choices=VERIFY)
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
    args.samples, args.workers = (int(v.strip() or 0) for v in (args.samples, args.workers))
    if args.samples < 1 or args.workers < 1:
        parser.error("--samples and --workers must be at least 1")
    args.min_agreement = float(args.min_agreement.strip() or 0)
    if not 0.5 < args.min_agreement <= 1:
        parser.error("--min-agreement is a share above 0.5 and at most 1")
    args.minutes = float(args.minutes.strip() or 0)
    return args


@dataclass(frozen=True)
class World:
    withheld: tuple[str, ...]
    removal: Plan
    added: list[tuple]


class Builder:
    def __init__(self, loaded: Loaded):
        self.loaded = loaded
        self.lineage = Lineage.of(loaded.rows, sessions(loaded.items))
        end = loaded.times[-1]
        self.known = tuple(sorted(i.id for i in loaded.items if i.knowable_at <= end))
        self.cache: dict[tuple, list[tuple]] = {}
        self.reached: dict[Reach, list[tuple]] = {}

    def build(self, spark, kept: Iterable[str]) -> World:
        kept = set(kept)
        withheld = tuple(i for i in self.known if i not in kept)
        removal = self.lineage.plan(withheld)
        parts = {
            (symbol, removal.inputs[symbol], removal.recompute[symbol]): Plan(
                (), (), {symbol: removal.recompute[symbol]}, {symbol: removal.inputs[symbol]}
            )
            for symbol in sorted(removal.inputs)
        }
        missing = {k: p for k, p in parts.items() if k not in self.cache}
        if missing:
            self.cache.update(recompute_each(spark, self.loaded.bars, missing))
        return World(withheld, removal, [r for k in parts for r in self.cache[k]])

    def rows(self, world: World, reach: Reach) -> list[tuple]:
        if reach not in self.reached:
            self.reached[reach] = in_reach(self.loaded.rows, reach)
        return apply(self.reached[reach], world.removal, in_reach(world.added, reach))

    def rebuilt(self, world: World) -> bool:
        loaded = self.loaded
        reference = rebuild(
            loaded.bars, loaded.fundamentals, loaded.features, world.withheld, HARNESS_REACH
        )
        extra, missing, _ = differences(self.rows(world, HARNESS_REACH), reference)
        return extra == 0 and missing == 0


def sample_until(decide, total: int, workers: int, stop) -> tuple[list[Sample], bool]:
    drawn: list[Sample] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while len(drawn) < total:
            batch = min(workers, total - len(drawn))
            drawn += pool.map(lambda _: timed(decide), range(batch))
            if len(drawn) < total and stop(drawn):
                return drawn, True
    return drawn, False


@dataclass(frozen=True)
class Tried:
    passed: bool
    valid: bool
    status: str
    calls: tuple[tuple[str, str], ...]
    input_hash: str
    rebuilt: bool | None = None


@dataclass(frozen=True)
class Trial:
    test_id: str
    phase: str
    kept: frozenset[str]
    dropped: tuple[str, ...]
    tried: Tried
    samples: list[Sample]
    stopped: bool
    decision: str | None
    effect: Effect
    removed: int
    recomputed: int
    seconds: float


def trial_row(args, point: Point, test: Trial, at: datetime) -> tuple:
    tried, effect = test.tried, test.effect
    return (
        test.test_id,
        point.key,
        args.run_id,
        at,
        test.phase,
        kept_hash(test.kept),
        len(test.kept),
        list(test.dropped),
        tried.status,
        tried.passed,
        tried.valid,
        len(test.samples),
        sum(1 for s in test.samples if not s.valid),
        test.stopped,
        test.decision,
        effect.outcomes,
        effect.p_values,
        [canonical(list(c)) for c in tried.calls],
        tried.input_hash,
        test.removed,
        test.recomputed,
        tried.rebuilt,
        test.seconds,
        args.code_commit,
        package_version(),
    )


class Search:
    def __init__(
        self,
        spark,
        point: Point,
        base: Base,
        builder: Builder,
        complete: Complete | None,
        args,
        writer: Writer,
        deadline: float,
        at: datetime,
        tried: dict[str, Tried],
    ):
        self.spark, self.point, self.base, self.builder = spark, point, base, builder
        self.complete, self.args, self.writer = complete, args, writer
        self.deadline, self.at = deadline, at
        self.tried = dict(tried)
        self.earlier = len(self.tried)
        self.screen = (base.calls, base.input_hash)
        self.smallest: int | None = None
        self.broken = sum(1 for t in self.tried.values() if t.rebuilt is False)

    def __call__(self, kept: frozenset[str], dropped: tuple[str, ...], phase: str) -> bool:
        found = self.tried.get(kept_hash(kept))
        if found is None:
            if time.monotonic() > self.deadline:
                raise Late
            found = self.test(kept, dropped, phase)
        if found.passed:
            self.smallest = len(kept)
            if found.status == SAMPLED:
                self.screen = (found.calls, found.input_hash)
        return found.passed

    def test(self, kept: frozenset[str], dropped: tuple[str, ...], phase: str) -> Tried:
        started = time.perf_counter()
        point, base, builder = self.point, self.base, self.builder
        world = builder.build(self.spark, kept)
        samples, stopped, decision = [], False, None
        if point.decider == STRATEGY_DECIDER:
            rows = builder.rows(world, STRATEGY_REACH)
            sample = strategy_sample(rows, point.times, point.params)
            samples, decision, seen, calls = [sample], sample.decision, sample.input_hash, ()
            effect = strategy_effect(base, sample.decision)
            passed, status = not effect.changed, EVALUATED
        else:
            tools = agent_tools(View(builder.rows(world, AGENT_REACH), point.at))
            calls, screen_hash = self.screen
            seen = inputs_hash(tools, calls)
            if seen == screen_hash:
                effect, passed, status = unchanged(base), True, SCREENED
            else:
                samples, stopped = sample_until(
                    lambda: run_agent(self.complete, tools, point.at),
                    point.samples,
                    self.args.workers,
                    lambda drawn: hopeless(base, drawn, point.samples, point.minimum),
                )
                effect = agent_effect(base, samples, point.minimum)
                decision = summarize([samples], point.minimum).decision
                calls = calls_of(samples)
                seen = inputs_hash(tools, calls)
                passed, status = not stopped and null_passed(effect), SAMPLED
        rebuilt = builder.rebuilt(world) if self.args.verify == VERIFY_ALL else None
        tried = Tried(passed, effect.valid, status, calls, seen, rebuilt)
        test = Trial(
            uuid.uuid4().hex,
            phase,
            kept,
            dropped,
            tried,
            samples,
            stopped,
            decision,
            effect,
            len(world.removal.removed),
            len(world.added),
            time.perf_counter() - started,
        )
        self.writer.add(SUFFICIENCY_TESTS, [trial_row(self.args, point, test, self.at)])
        if status == SAMPLED:
            self.writer.add(
                DECISION_SAMPLES,
                samples_of(self.args.run_id, test.test_id, point.decider, [samples], self.at),
            )
        self.writer.flush(force=False)
        if rebuilt is False:
            self.broken += 1
        if tried.valid:
            self.tried[kept_hash(kept)] = tried
        return tried

    @property
    def tests(self) -> int:
        return len(self.tried)

    @property
    def sampled(self) -> int:
        return sum(1 for t in self.tried.values() if t.status == SAMPLED)


@dataclass(frozen=True)
class Settled:
    kept: tuple[str, ...]
    tests: int
    sampled: int
    confirm_id: str
    decision: str | None
    confirmed: bool
    rebuilt: bool | None
    ablation_id: str | None
    coherent: bool | None


def confirm(
    spark,
    point: Point,
    base: Base,
    loaded: Loaded,
    builder: Builder,
    kept: frozenset[str],
    complete: Complete | None,
    args,
    writer: Writer,
    at: datetime,
    search: Search,
) -> Settled:
    world = builder.build(spark, kept)
    confirm_id = uuid.uuid4().hex
    if point.decider == STRATEGY_DECIDER:
        rows = builder.rows(world, STRATEGY_REACH)
        rounds = draw(lambda: strategy_sample(rows, point.times, point.params), 2, 1, 1)
        again = strategy_base([s for r in rounds for s in r])
        decision = again.decision
        confirmed = again.reproducible and not strategy_effect(base, decision).changed
    else:
        tools = agent_tools(View(builder.rows(world, AGENT_REACH), point.at))
        rounds = draw(lambda: run_agent(complete, tools, point.at), 2, point.samples, args.workers)
        summary = summarize(rounds, point.minimum)
        every = [s for r in rounds for s in r]
        decision = summary.decision
        same = all(summary.settled.get(k) == v for k, v in base.settled.items())
        quiet = null_passed(agent_effect(base, every, point.minimum))
        confirmed = summary.reproducible(point.minimum) and same and quiet
        writer.add(DECISION_SAMPLES, samples_of(args.run_id, confirm_id, point.decider, rounds, at))
    rebuilt = builder.rebuilt(world)
    restricted = replace(loaded, rows=in_reach(loaded.rows, REACH))
    outcome = ablate(spark, restricted, world.withheld, COHERENT)
    recorded = point.record["input_hash"]
    noted = SimpleNamespace(run_id=args.run_id, mode=COHERENT, code_commit=args.code_commit)
    base_hash = information_set(loaded.items, loaded.times[-1]).hash
    writer.add(ABLATION_CHANGES, change_rows(outcome))
    writer.add(ABLATIONS, [ablation_row(noted, outcome, [point.record], loaded, base_hash, at)])
    return Settled(
        tuple(sorted(kept)),
        search.tests,
        search.sampled,
        confirm_id,
        decision,
        confirmed,
        rebuilt,
        outcome.ablation_id,
        outcome.coherent(recorded),
    )


def set_row(args, point: Point, candidates: int, settled: Settled, at: datetime) -> tuple:
    return (
        point.key,
        args.run_id,
        at,
        point.decider,
        point.at,
        candidates,
        list(settled.kept),
        by_kind(settled.kept),
        kept_hash(settled.kept),
        settled.tests,
        settled.sampled,
        settled.confirm_id,
        settled.decision,
        settled.confirmed,
        settled.rebuilt,
        settled.ablation_id,
        settled.coherent,
        args.code_commit,
        package_version(),
    )


def stored_tests_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, kept_hash, passed, valid, status, calls, input_hash, rebuilt "
        f"FROM {table_fqn(catalog, 'control', SUFFICIENCY_TESTS)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_sets_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, kept, tests, sampled, confirm_id, confirm_decision, confirmed, "
        f"rebuilt, ablation_id, coherent FROM {table_fqn(catalog, 'control', SUFFICIENT_SETS)} "
        f"WHERE point_key IN ({keys_in(keys)}) ORDER BY created_at"
    )


def stored_tried(row) -> Tried:
    return Tried(
        row["passed"],
        row["valid"],
        row["status"],
        tuple(tuple(json.loads(c)) for c in row["calls"]),
        row["input_hash"],
        row["rebuilt"],
    )


def stored_set(row) -> Settled:
    return Settled(
        tuple(row["kept"]),
        row["tests"],
        row["sampled"],
        row["confirm_id"],
        row["confirm_decision"],
        row["confirmed"],
        row["rebuilt"],
        row["ablation_id"],
        row["coherent"],
    )


@dataclass(frozen=True)
class Progress:
    point: Point
    base: Base | None
    candidates: int
    tests: int
    earlier: int
    sampled: int
    smallest: int | None
    broken: int
    settled: Settled | None
    outline: tuple[str, ...] = ()

    @property
    def problems(self) -> list[str]:
        found = []
        if self.base is not None and not self.base.reproducible:
            found.append("its decision in the real world is not reproducible")
        if self.broken:
            found.append(f"{self.broken} worlds tried differ from the full rebuild")
        settled = self.settled
        if settled is not None:
            if not settled.confirmed:
                found.append("the world built from the set alone does not decide the same")
            if settled.rebuilt is False:
                found.append("the world built from the set alone differs from the full rebuild")
            if settled.coherent is False:
                found.append(f"its ablation {settled.ablation_id} is not coherent")
        return found

    @property
    def state(self) -> str:
        if self.problems:
            return FAILED
        return SUFFICIENT if self.settled is not None else PARTIAL


def parts_text(base: Base) -> str:
    return ", ".join(
        f"{k} {v}" if isinstance(v, str) else f"{k} {v:.4f}"
        for k, v in sorted(base.settled.items())
    )


def describe(progress: Progress) -> str:
    point, base, settled = progress.point, progress.base, progress.settled
    if base is None:
        return f"{point.label}: no decision in the real world yet"
    head = (
        f"{point.label} (point {point.key[:SHORT]}): settled {parts_text(base) or 'nothing'}; "
        f"{progress.candidates:,} items knowable"
    )
    if settled is None:
        smallest = progress.candidates if progress.smallest is None else progress.smallest
        lines = [
            f"{head}; search stopped after {progress.tests:,} worlds ({progress.earlier:,} from "
            f"earlier runs, {progress.sampled:,} sampled), {smallest:,} items still kept"
        ]
    else:
        kinds = ", ".join(f"{k} {n}" for k, n in by_kind(settled.kept).items())
        decided = "decides the same" if settled.confirmed else "DECIDES DIFFERENTLY"
        rebuilt = {None: "not checked", True: "equal", False: "DIFFERENT"}[settled.rebuilt]
        coherent = {None: "not checked", True: "coherent", False: "INCOHERENT"}[settled.coherent]
        lines = [
            f"{head}; {len(settled.kept):,} are enough and none of them can go ({kinds}); "
            f"{settled.tests:,} worlds tried, {settled.sampled:,} sampled",
            f"  the set alone {decided}: {settled.decision}; rebuild {rebuilt}; ablation "
            f"{settled.ablation_id} {coherent}",
            *(f"  {line}" for line in progress.outline),
        ]
    lines += [f"  problem: {p}" for p in progress.problems]
    return "\n".join(lines)


def suffice_point(
    spark,
    point: Point,
    loaded: Loaded,
    builder: Builder,
    base: Base | None,
    tried: dict[str, Tried],
    settled: Settled | None,
    complete: Complete | None,
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Progress:
    info = information_set(loaded.items, point.at)
    items: list[Item] = sorted(info.items.values(), key=order)

    def progress(search: Search | None = None, found: Settled | None = None) -> Progress:
        shown = outline(info.items[i] for i in found.kept if i in info.items) if found else ()
        if search is None:
            counted = found.tests if found else len(tried)
            sampled = found.sampled if found else sum(t.status == SAMPLED for t in tried.values())
            return Progress(
                point, base, len(items), counted, counted, sampled, None, 0, found, tuple(shown)
            )
        return Progress(
            point,
            base,
            len(items),
            search.tests,
            search.earlier,
            search.sampled,
            search.smallest,
            search.broken,
            found,
            tuple(shown),
        )

    def late() -> bool:
        return time.monotonic() > deadline

    if settled is not None:
        return progress(found=settled)
    if base is None:
        if late():
            return progress()
        base, rounds, verified = base_of(point, loaded, complete, args.workers)
        world_id = uuid.uuid4().hex
        writer.add(ATTRIBUTION_POINTS, [point_row(args, point, base, world_id, info, verified, at)])
        if point.decider == AGENT_DECIDER:
            writer.add(
                DECISION_SAMPLES, samples_of(args.run_id, world_id, point.decider, rounds, at)
            )
        writer.flush()
        if verified is False:
            print(f"  {point.label}: inputs are NOT equal to the recorded hash")
    if not base.reproducible:
        return progress()
    search = Search(spark, point, base, builder, complete, args, writer, deadline, at, tried)
    try:
        kept = reduce(items, search)
    except Late:
        return progress(search)
    if late():
        return progress(search)
    found = confirm(spark, point, base, loaded, builder, kept, complete, args, writer, at, search)
    writer.add(SUFFICIENT_SETS, [set_row(args, point, len(items), found, at)])
    writer.flush()
    return progress(search, found)


def suffice_all(spark, args, at: datetime) -> list[Progress]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    loaded = load(spark, args.catalog, next(iter(group_decisions([record]))), HARNESS_REACH)
    points = [
        make_point(d, record, day, loaded.versions, args) for day in args.at for d in args.deciders
    ]
    keys = [p.key for p in points]
    bases = {
        r["point_key"]: stored_base(r)
        for r in spark.sql(stored_points_query(args.catalog, keys)).collect()
    }
    tried: dict[str, dict[str, Tried]] = {}
    for row in spark.sql(stored_tests_query(args.catalog, keys)).collect():
        tried.setdefault(row["point_key"], {})[row["kept_hash"]] = stored_tried(row)
    sets = {
        r["point_key"]: stored_set(r)
        for r in spark.sql(stored_sets_query(args.catalog, keys)).collect()
    }
    complete = endpoint_caller(args.endpoint) if AGENT_DECIDER in args.deciders else None
    builder, writer = Builder(loaded), Writer(spark, args.catalog, SCHEMAS)
    deadline = time.monotonic() + args.minutes * 60
    found = []
    for point in points:
        progress = suffice_point(
            spark,
            point,
            loaded,
            builder,
            bases.get(point.key),
            tried.get(point.key, {}),
            sets.get(point.key),
            complete,
            args,
            writer,
            deadline,
            at,
        )
        writer.flush()
        print(f"{progress.state}: {describe(progress)}")
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

    found = suffice_all(spark, args, datetime.now(UTC))
    failures = [f"{p.point.label}: {'; '.join(p.problems)}" for p in found if p.state == FAILED]
    if failures:
        raise SufficiencyFailed(failures)
    left = [p for p in found if p.state == PARTIAL]
    if left:
        print(
            f"{PARTIAL}: {len(left)} of {len(found)} decisions still have worlds to try; run "
            "the job again with the same parameters to continue"
        )
        return
    print(
        f"OK: {len(found)} decisions in {args.catalog} have a minimal sufficient set, each "
        "deciding the same from that set alone"
    )


if __name__ == "__main__":
    main()

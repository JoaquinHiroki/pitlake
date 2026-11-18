import argparse
import re
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pitlake.attribution import (
    EVALUATED,
    SAMPLED,
    SCREENED,
    Base,
    Effect,
    agent_effect,
    inputs_hash,
    null_passed,
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
    run_agent,
    strategy_sample,
    summarize,
)
from pitlake.information import Item, information_set
from pitlake.jobs.attribute import (
    Point,
    Record,
    Writer,
    keys_in,
    make_point,
    new_base,
    stored_base,
    stored_points_query,
    stored_record,
    stored_rows_query,
)
from pitlake.jobs.decide import draw, endpoint_caller, samples_of
from pitlake.jobs.information_set import SHORT, Loaded, decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.jobs.suffice import Builder, World, ablation_of
from pitlake.tables import (
    ABLATION_CHANGES,
    ABLATION_CHANGES_SCHEMA,
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
    SOURCE_TESTS,
    SOURCE_TESTS_SCHEMA,
    SOURCE_VALUES,
    SOURCE_VALUES_SCHEMA,
)
from pitlake.value import (
    EVERY,
    FEED,
    LEVELS,
    NONE,
    NOTHING,
    Source,
    Tally,
    Value,
    combine,
    sources,
    tally,
    value,
)

VALUED = "VALUED"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
SCHEMAS = {
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    ATTRIBUTION_POINTS: ATTRIBUTION_POINTS_SCHEMA,
    ABLATION_CHANGES: ABLATION_CHANGES_SCHEMA,
    ABLATIONS: ABLATIONS_SCHEMA,
    SOURCE_TESTS: SOURCE_TESTS_SCHEMA,
}
NULL_WORLD = Source(NONE, NONE, NONE, frozenset())


class ValueFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decisions failed data value: {'; '.join(failures)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", default="", help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10", help="Agent samples per world")
    parser.add_argument("--min-agreement", default="0.8")
    parser.add_argument("--levels", default=" ".join(LEVELS), help="feed and/or series")
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
    args.levels = tuple(dict.fromkeys(v for v in re.split(r"[\s,;]+", args.levels) if v))
    wrong = sorted(set(args.levels) - set(LEVELS))
    if wrong or not args.levels:
        parser.error(f"--levels must name {' and/or '.join(LEVELS)}, not {wrong}")
    args.samples, args.workers = (int(v.strip() or 0) for v in (args.samples, args.workers))
    if args.samples < 1 or args.workers < 1:
        parser.error("--samples and --workers must be at least 1")
    args.min_agreement = float(args.min_agreement.strip() or 0)
    if not 0 <= args.min_agreement <= 1:
        parser.error("--min-agreement is a share between 0 and 1")
    args.minutes = float(args.minutes.strip() or 0)
    return args


def venues_of(bars) -> dict[str, str]:
    found: dict[str, str] = {}
    for row in bars.select("venue", "symbol").distinct().collect():
        if found.setdefault(row["symbol"], row["venue"]) != row["venue"]:
            raise ValueError(f"{row['symbol']} trades on two venues; a symbol has one")
    return found


@dataclass(frozen=True)
class Check:
    rebuilt: bool | None
    ablation_id: str | None
    coherent: bool | None


@dataclass(frozen=True)
class Judged:
    source: str
    status: str
    inputs_changed: bool
    effect: Effect
    check: Check


class Worlds:
    def __init__(self, spark, loaded: Loaded, args, writer: Writer, at: datetime):
        self.spark, self.loaded, self.args, self.writer, self.at = spark, loaded, args, writer, at
        self.builder = Builder(loaded)
        self.built: dict[str, World] = {}
        self.checks: dict[str, Check] = {}

    def kept(self, source: Source) -> list[str]:
        return [i for i in self.builder.known if i not in source.items]

    def world(self, source: Source) -> World:
        key = source.withheld_hash
        if key not in self.built:
            self.built[key] = self.builder.build(self.spark, self.kept(source))
        return self.built[key]

    def rows(self, source: Source, reach) -> list[tuple]:
        return self.builder.rows(self.world(source), reach)

    def check(self, point: Point, source: Source) -> Check:
        key = source.withheld_hash
        if key not in self.checks:
            world = self.world(source)
            rebuilt = self.builder.rebuilt(world)
            ablation_id, coherent = ablation_of(
                self.spark, point, self.loaded, world.withheld, self.args, self.writer, self.at
            )
            self.checks[key] = Check(rebuilt, ablation_id, coherent)
        return self.checks[key]


@dataclass(frozen=True)
class Decided:
    test_id: str
    source: Source
    status: str
    input_hash: str
    samples: list[Sample]
    decision: str | None
    effect: Effect
    removed: int
    recomputed: int
    check: Check
    seconds: float


def decide(
    point: Point, base: Base, worlds: Worlds, source: Source, complete: Complete | None, args
) -> Decided:
    started = time.perf_counter()
    samples, decision = [], None
    if point.decider == STRATEGY_DECIDER:
        sample = strategy_sample(worlds.rows(source, STRATEGY_REACH), point.times, point.params)
        samples, decision, seen = [sample], sample.decision, sample.input_hash
        effect, status = strategy_effect(base, sample.decision), EVALUATED
    else:
        tools = agent_tools(View(worlds.rows(source, AGENT_REACH), point.at))
        seen = inputs_hash(tools, base.calls)
        if source.items and seen == base.input_hash:
            effect, status = unchanged(base), SCREENED
        else:
            samples = draw(
                lambda: run_agent(complete, tools, point.at), 1, point.samples, args.workers
            )[0]
            decision = summarize([samples], point.minimum).decision
            effect, status = agent_effect(base, samples, point.minimum), SAMPLED
    world = worlds.world(source)
    check = worlds.check(point, source)
    return Decided(
        uuid.uuid4().hex,
        source,
        status,
        seen,
        samples,
        decision,
        effect,
        len(world.removal.removed),
        len(world.added),
        check,
        time.perf_counter() - started,
    )


def knowable(source: Source, items: dict[str, Item], at: datetime) -> int:
    return sum(1 for i in source.items if items[i].knowable_at <= at)


def source_row(args, point: Point, base: Base, found: Decided, count: int, at: datetime) -> tuple:
    source, effect, check = found.source, found.effect, found.check
    return (
        found.test_id,
        point.key,
        args.run_id,
        at,
        point.decider,
        point.at,
        source.name,
        source.level,
        source.feed,
        source.withheld_hash,
        len(source.items),
        count,
        found.status,
        found.input_hash,
        found.input_hash != base.input_hash,
        len(found.samples),
        sum(1 for s in found.samples if not s.valid),
        found.decision,
        effect.outcomes,
        effect.effects,
        effect.p_values,
        list(effect.caused),
        effect.valid,
        found.removed,
        found.recomputed,
        check.rebuilt,
        check.ablation_id,
        check.coherent,
        found.seconds,
        args.code_commit,
        package_version(),
    )


def stored_tests_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, source, withheld_hash, status, inputs_changed, outcomes, effects, "
        "p_values, caused, valid, rebuilt, ablation_id, coherent "
        f"FROM {table_fqn(catalog, 'control', SOURCE_TESTS)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_judged(row) -> Judged:
    effect = Effect(
        dict(row["outcomes"]),
        dict(row["effects"]),
        dict(row["p_values"]),
        tuple(row["caused"]),
        row["valid"],
    )
    check = Check(row["rebuilt"], row["ablation_id"], row["coherent"])
    return Judged(row["source"], row["status"], row["inputs_changed"], effect, check)


@dataclass(frozen=True)
class Progress:
    point: Point
    base: Base | None
    worlds: tuple[Source, ...]
    tested: dict[str, Judged]
    knowable: dict[str, int]
    records: list[Record] = field(default_factory=list)

    @property
    def left(self) -> int:
        return sum(1 for s in self.worlds if s.withheld_hash not in self.tested)

    @property
    def null(self) -> Judged | None:
        return self.tested.get(NULL_WORLD.withheld_hash)

    @property
    def problems(self) -> list[str]:
        found = []
        if self.base is not None and not self.base.reproducible:
            found.append("its decision in the real world is not reproducible")
        if self.null is not None and not null_passed(self.null.effect):
            found.append("the null world changed it")
        for tested in self.tested.values():
            if tested.check.rebuilt is False:
                found.append(f"the world without {tested.source} differs from the full rebuild")
            if tested.check.coherent is False:
                found.append(
                    f"the ablation {tested.check.ablation_id} without {tested.source} is not "
                    "coherent"
                )
        return found

    @property
    def state(self) -> str:
        if self.problems:
            return FAILED
        return VALUED if self.base is not None and not self.left else PARTIAL

    def effect(self, source: Source) -> Effect:
        return self.tested[source.withheld_hash].effect


def parts_text(effect: Effect, parts: Iterable[str]) -> str:
    return ", ".join(f"{p} {effect.outcomes[p]} {effect.effects[p]:+.4f}" for p in parts)


def describe(progress: Progress) -> str:
    point, base = progress.point, progress.base
    if base is None:
        return f"{point.label}: no decision in the real world yet"
    done = [s for s in progress.worlds if s.withheld_hash in progress.tested]
    lines = [
        f"{point.label} (point {point.key[:SHORT]}): {len(progress.worlds) - 1} sources; "
        f"{len(done):,} worlds decided, {progress.left:,} left"
    ]
    for source in done:
        tested = progress.tested[source.withheld_hash]
        effect, check = tested.effect, tested.check
        name = "null world" if source == NULL_WORLD else f"without {source.name}"
        state = "same" if null_passed(effect) else "CHANGED" if effect.caused else "unclear"
        checked = {None: "not checked", True: "coherent", False: "INCOHERENT"}[check.coherent]
        line = (
            f"  {name} ({progress.knowable.get(source.name, 0):,} items, {tested.status}): "
            f"{state}; ablation {checked}"
        )
        if effect.caused:
            line += f"; {parts_text(effect, effect.caused)}"
        lines.append(line)
    lines += [f"  problem: {p}" for p in progress.problems]
    return "\n".join(lines)


def value_point(
    point: Point,
    loaded: Loaded,
    worlds: Worlds,
    found: tuple[Source, ...],
    base: Base | None,
    tested: dict[str, Judged],
    records: list[Record],
    complete: Complete | None,
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Progress:
    info = information_set(loaded.items, point.at)
    items = {i.id: i for i in loaded.items}
    every = (NULL_WORLD, *found)
    counts = {s.name: knowable(s, items, point.at) for s in found}
    tested = dict(tested)

    def progress() -> Progress:
        return Progress(point, base, every, tested, counts, records)

    if base is None:
        if time.monotonic() > deadline:
            return progress()
        base = new_base(point, loaded, info, complete, args, writer, at)
    if not base.reproducible:
        return progress()
    for source in every:
        if source.withheld_hash in tested:
            continue
        if time.monotonic() > deadline:
            break
        decided = decide(point, base, worlds, source, complete, args)
        count = counts.get(source.name, 0)
        writer.add(SOURCE_TESTS, [source_row(args, point, base, decided, count, at)])
        if decided.status == SAMPLED:
            writer.add(
                DECISION_SAMPLES,
                samples_of(args.run_id, decided.test_id, point.decider, [decided.samples], at),
            )
        writer.flush(force=False)
        if decided.effect.valid:
            tested[source.withheld_hash] = Judged(
                source.name,
                decided.status,
                decided.input_hash != base.input_hash,
                decided.effect,
                decided.check,
            )
    return progress()


def values_of(found: list[Progress], every: tuple[Source, ...]) -> list[Value]:
    result = []
    for source in every:
        mine = []
        for decider in DECIDERS:
            progress = [p for p in found if p.point.decider == decider]
            if not progress:
                continue
            effects = [p.effect(source) for p in progress]
            total = Tally()
            for p in progress:
                total += tally(p.records, source)
            count = max(p.knowable[source.name] for p in progress)
            mine.append(value(decider, source, effects, count, total))
        result += mine
        if len(mine) > 1:
            result.append(combine(mine))
    return result


def value_line(found: Value) -> str:
    source, loo = found.source, found.one_at_a_time
    level = "feed" if source.level == FEED else f"series of {source.feed}"
    decider = "every decider" if found.decider == EVERY else found.decider
    line = (
        f"  {source.name} ({level}, {found.knowable:,} items), {decider}: changes "
        f"{found.changed} of {found.decisions} decisions, {found.parts_changed} of "
        f"{found.parts} parts, largest effect {found.largest:.4f}"
    )
    if loo.attributed:
        line += (
            f"; one at a time {loo.caused:,} of {loo.attributed:,} attributed items changed a part"
        )
    return f"{line}; {found.verdict}"


def value_rows(args, found: list[Value], days: list, backtest: str, at: datetime) -> list[tuple]:
    return [
        (
            args.run_id,
            at,
            backtest,
            v.decider,
            v.decisions,
            days,
            v.source.name,
            v.source.level,
            v.source.feed,
            v.source.withheld_hash,
            v.knowable,
            v.changed,
            v.parts,
            v.parts_changed,
            v.largest,
            v.one_at_a_time.attributed,
            v.one_at_a_time.inputs_changed,
            v.one_at_a_time.caused,
            sorted(v.one_at_a_time.parts),
            v.verdict,
            args.code_commit,
            package_version(),
        )
        for v in found
    ]


def value_all(spark, args, at: datetime) -> tuple[list[Progress], tuple[Source, ...], dict]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    loaded = load(spark, args.catalog, next(iter(group_decisions([record]))), HARNESS_REACH)
    known = information_set(loaded.items, loaded.times[-1]).items.values()
    found = tuple(sources(known, venues_of(loaded.bars), args.levels))
    points = [
        make_point(d, record, day, loaded.versions, args) for day in args.at for d in args.deciders
    ]
    keys = [p.key for p in points]
    bases = {
        r["point_key"]: stored_base(r)
        for r in spark.sql(stored_points_query(args.catalog, keys)).collect()
    }
    tested: dict[str, dict[str, Judged]] = {}
    for row in spark.sql(stored_tests_query(args.catalog, keys)).collect():
        tested.setdefault(row["point_key"], {})[row["withheld_hash"]] = stored_judged(row)
    attributed: dict[str, list[Record]] = {}
    for row in spark.sql(stored_rows_query(args.catalog, keys)).collect():
        attributed.setdefault(row["point_key"], []).append(stored_record(row))
    complete = endpoint_caller(args.endpoint) if AGENT_DECIDER in args.deciders else None
    writer = Writer(spark, args.catalog, SCHEMAS)
    worlds = Worlds(spark, loaded, args, writer, at)
    deadline = time.monotonic() + args.minutes * 60
    progress = []
    for point in points:
        done = value_point(
            point,
            loaded,
            worlds,
            found,
            bases.get(point.key),
            tested.get(point.key, {}),
            attributed.get(point.key, []),
            complete,
            args,
            writer,
            deadline,
            at,
        )
        writer.flush()
        print(f"{done.state}: {describe(done)}")
        progress.append(done)
    return progress, found, record


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)

    at = datetime.now(UTC)
    found, every, record = value_all(spark, args, at)
    failures = [f"{p.point.label}: {'; '.join(p.problems)}" for p in found if p.state == FAILED]
    if failures:
        raise ValueFailed(failures)
    left = [p for p in found if p.state == PARTIAL]
    if left:
        print(
            f"{PARTIAL}: {len(left)} of {len(found)} decisions still have worlds to decide; run "
            "the job again with the same parameters to continue"
        )
        return
    values = values_of(found, every)
    days = sorted({p.point.at.date() for p in found})
    print(
        f"VALUE: {len(found)} decisions at {', '.join(map(str, days))}; a source changes a "
        "decision when withholding all of it changes a part (for the agent at p < 0.05), and "
        "changes nothing when every part stays the same"
    )
    for v in values:
        print(value_line(v))
    writer = Writer(spark, args.catalog, {SOURCE_VALUES: SOURCE_VALUES_SCHEMA})
    writer.add(SOURCE_VALUES, value_rows(args, values, days, record["backtest_id"], at))
    writer.flush()
    widest = EVERY if len(args.deciders) > 1 else args.deciders[0]
    quiet = [v.source.name for v in values if v.decider == widest and v.verdict == NOTHING]
    print(
        f"OK: {len(found)} decisions valued in {args.catalog}; {len(quiet)} of {len(every)} "
        f"sources change no decision in the sample: {', '.join(quiet) or 'none'}"
    )


if __name__ == "__main__":
    main()

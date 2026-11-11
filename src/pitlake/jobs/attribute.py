import argparse
import json
import re
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from pitlake.ablation import Lineage, Plan, apply, differences, rebuild, recompute_each
from pitlake.attribution import (
    BARS,
    EVALUATED,
    NULL,
    SAMPLED,
    SCREENED,
    VERIFY,
    VERIFY_ALL,
    VERIFY_CHANGED,
    Base,
    Effect,
    agent_base,
    agent_effect,
    calls_of,
    inputs_hash,
    null_passed,
    select,
    strategy_base,
    strategy_effect,
    unchanged,
)
from pitlake.backtest import Params, fingerprint
from pitlake.bronze import parse_optional_date, sql_string
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
    agent_config,
    agent_tools,
    canonical,
    in_reach,
    run_agent,
    strategy_config,
    strategy_sample,
    summarize,
)
from pitlake.information import InformationSet, Item, Reach, information_set, sessions
from pitlake.jobs.decide import draw, endpoint_caller, instant, samples_of
from pitlake.jobs.information_set import SHORT, Loaded, decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.tables import (
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    ATTRIBUTIONS,
    ATTRIBUTIONS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
)

FLUSH_SECONDS = 600
TOP = 10
ATTRIBUTED = "ATTRIBUTED"
PARTIAL = "PARTIAL"
FAILED = "FAILED"


class AttributionFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decisions failed attribution: {'; '.join(failures)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", default="", help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10", help="Agent samples per ablated world")
    parser.add_argument("--min-agreement", default="0.8")
    parser.add_argument("--bars", default=str(BARS), help="Changed bars per symbol the agent tries")
    parser.add_argument("--verify", default=VERIFY_CHANGED, choices=VERIFY)
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
    args.samples, args.bars, args.workers = (
        int(v.strip() or 0) for v in (args.samples, args.bars, args.workers)
    )
    if args.samples < 1 or args.workers < 1:
        parser.error("--samples and --workers must be at least 1")
    args.min_agreement = float(args.min_agreement.strip() or 0)
    if not 0 <= args.min_agreement <= 1:
        parser.error("--min-agreement is a share between 0 and 1")
    args.minutes = float(args.minutes.strip() or 0)
    return args


@dataclass(frozen=True)
class Point:
    decider: str
    record: dict
    at: datetime
    times: list[datetime]
    config: dict
    config_hash: str
    versions: dict
    key: str
    samples: int
    minimum: float

    @property
    def params(self) -> Params:
        return Params.from_json(self.record["params"])

    @property
    def label(self) -> str:
        return f"{self.decider} at {self.at:%Y-%m-%d %H:%M} UTC"


def make_point(decider: str, record: dict, day: date | None, versions: dict, args) -> Point:
    at, times = instant(record, day)
    if decider == STRATEGY_DECIDER:
        config, samples = strategy_config(Params.from_json(record["params"])), 1
    else:
        config, samples = agent_config(args.endpoint), args.samples
    config_hash = fingerprint(config)
    key = fingerprint(
        [
            decider,
            config_hash,
            at.isoformat(),
            sorted(versions.items()),
            samples,
            args.min_agreement,
        ]
    )
    return Point(
        decider,
        record,
        at,
        times,
        config,
        config_hash,
        dict(versions),
        key,
        samples,
        args.min_agreement,
    )


class Worlds:
    def __init__(self, loaded: Loaded):
        self.loaded = loaded
        self.lineage = Lineage.of(loaded.rows, sessions(loaded.items))
        self.plans: dict[str, Plan] = {}
        self.added: dict[str, list[tuple]] = {}
        self.reached: dict[Reach, list[tuple]] = {}

    def prepare(self, spark, items: Iterable[str]) -> None:
        new = {i: self.lineage.plan((i,)) for i in items if i not in self.plans}
        self.plans.update(new)
        windows = {i: p for i, p in new.items() if p.inputs}
        if windows:
            self.added.update(recompute_each(spark, self.loaded.bars, windows))

    def plan(self, item: str | None) -> Plan:
        return self.lineage.plan(()) if item is None else self.plans[item]

    def rows(self, item: str | None, reach: Reach) -> list[tuple]:
        if reach not in self.reached:
            self.reached[reach] = in_reach(self.loaded.rows, reach)
        added = [] if item is None else self.added.get(item, [])
        return apply(self.reached[reach], self.plan(item), in_reach(added, reach))

    def rebuilt(self, item: str | None) -> bool:
        withheld = () if item is None else (item,)
        loaded = self.loaded
        reference = rebuild(
            loaded.bars, loaded.fundamentals, loaded.features, withheld, HARNESS_REACH
        )
        extra, missing, _ = differences(self.rows(item, HARNESS_REACH), reference)
        return extra == 0 and missing == 0


def base_of(
    point: Point, loaded: Loaded, complete: Complete | None, workers: int
) -> tuple[Base, list[list[Sample]], bool | None]:
    if point.decider == STRATEGY_DECIDER:
        rows = in_reach(loaded.rows, STRATEGY_REACH)
        rounds = draw(lambda: strategy_sample(rows, point.times, point.params), 1, 2, 1)
        base = strategy_base(rounds[0])
        verified = None
        if point.times[-1].date() == point.record["period_end"]:
            verified = base.input_hash == point.record["input_hash"]
        return base, rounds, verified
    view = View(in_reach(loaded.rows, AGENT_REACH), point.at)
    rounds = draw(
        lambda: run_agent(complete, agent_tools(view), point.at), 2, point.samples, workers
    )
    every = [s for r in rounds for s in r]
    seen = inputs_hash(agent_tools(view), calls_of(every))
    return agent_base(summarize(rounds, point.minimum), every, point.minimum, seen), rounds, None


@dataclass(frozen=True)
class Record:
    withheld: tuple[str, ...]
    status: str
    inputs_changed: bool
    effect: Effect
    rebuilt: bool | None


@dataclass(frozen=True)
class Attributed:
    world_id: str
    item: Item | None
    status: str
    input_hash: str
    samples: list[Sample]
    decision: str | None
    effect: Effect
    removed: int
    recomputed: int
    rebuilt: bool | None
    seconds: float

    @property
    def withheld(self) -> tuple[str, ...]:
        return () if self.item is None else (self.item.id,)

    def record(self, base: Base) -> Record:
        changed = self.input_hash != base.input_hash
        return Record(self.withheld, self.status, changed, self.effect, self.rebuilt)


def agent_inputs(point: Point, base: Base, worlds: Worlds, item: str | None) -> tuple:
    view = View(worlds.rows(item, AGENT_REACH), point.at)
    tools = agent_tools(view)
    return tools, inputs_hash(tools, base.calls)


def evaluate(
    point: Point,
    base: Base,
    worlds: Worlds,
    item: Item | None,
    complete: Complete | None,
    args,
    inputs: tuple | None = None,
) -> Attributed:
    started = time.perf_counter()
    found = None if item is None else item.id
    samples, decision = [], None
    if point.decider == STRATEGY_DECIDER:
        sample = strategy_sample(worlds.rows(found, STRATEGY_REACH), point.times, point.params)
        samples, decision, seen = [sample], sample.decision, sample.input_hash
        effect, status = strategy_effect(base, sample.decision), EVALUATED
    else:
        tools, seen = inputs or agent_inputs(point, base, worlds, found)
        if item is not None and seen == base.input_hash:
            effect, status = unchanged(base), SCREENED
        else:
            samples = draw(
                lambda: run_agent(complete, tools, point.at), 1, point.samples, args.workers
            )[0]
            decision = summarize([samples], point.minimum).decision
            effect, status = agent_effect(base, samples, point.minimum), SAMPLED
    verify = args.verify == VERIFY_ALL or (
        args.verify == VERIFY_CHANGED and (item is None or effect.changed)
    )
    return Attributed(
        uuid.uuid4().hex,
        item,
        status,
        seen,
        samples,
        decision,
        effect,
        len(worlds.plan(found).removed),
        len(worlds.added.get(found, [])) if found else 0,
        worlds.rebuilt(found) if verify else None,
        time.perf_counter() - started,
    )


def point_row(
    args,
    point: Point,
    base: Base,
    world_id: str,
    info: InformationSet,
    verified: bool | None,
    at: datetime,
) -> tuple:
    record = point.record
    return (
        point.key,
        args.run_id,
        at,
        point.decider,
        canonical(point.config),
        point.config_hash,
        record["backtest_id"],
        record["research_run_id"],
        point.at,
        point.versions,
        info.hash,
        len(info.items),
        info.by_kind(),
        point.samples,
        point.minimum,
        world_id,
        base.decision,
        canonical(base.settled),
        base.support,
        base.valid,
        [canonical(list(c)) for c in base.calls],
        base.input_hash,
        base.reproducible,
        verified,
        args.code_commit,
        package_version(),
    )


def attribution_row(args, point: Point, base: Base, found: Attributed, at: datetime) -> tuple:
    item, effect = found.item, found.effect
    return (
        found.world_id,
        point.key,
        args.run_id,
        at,
        list(found.withheld),
        NULL if item is None else item.kind,
        None if item is None else item.entity,
        None if item is None else item.knowable_at,
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
        found.rebuilt,
        found.seconds,
        args.code_commit,
        package_version(),
    )


def keys_in(keys: Iterable[str]) -> str:
    return ", ".join(sql_string(k) for k in sorted(set(keys)))


def stored_points_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, base_decision, settled, support, valid, calls, input_hash, "
        f"reproducible FROM {table_fqn(catalog, 'control', ATTRIBUTION_POINTS)} "
        f"WHERE point_key IN ({keys_in(keys)}) ORDER BY created_at DESC"
    )


def stored_rows_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, withheld, status, inputs_changed, outcomes, effects, p_values, "
        f"caused, valid, rebuilt FROM {table_fqn(catalog, 'control', ATTRIBUTIONS)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_base(row) -> Base:
    return Base(
        row["base_decision"],
        json.loads(row["settled"]),
        dict(row["support"]),
        row["valid"],
        tuple(tuple(json.loads(c)) for c in row["calls"]),
        row["input_hash"],
        row["reproducible"],
    )


def stored_record(row) -> Record:
    effect = Effect(
        dict(row["outcomes"]),
        dict(row["effects"]),
        dict(row["p_values"]),
        tuple(row["caused"]),
        row["valid"],
    )
    return Record(
        tuple(row["withheld"]), row["status"], row["inputs_changed"], effect, row["rebuilt"]
    )


SCHEMAS = {
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    ATTRIBUTION_POINTS: ATTRIBUTION_POINTS_SCHEMA,
    ATTRIBUTIONS: ATTRIBUTIONS_SCHEMA,
}


class Writer:
    def __init__(self, spark, catalog: str, schemas: dict[str, str]):
        self.spark, self.catalog, self.schemas = spark, catalog, schemas
        self.pending: dict[str, list[tuple]] = {table: [] for table in schemas}
        self.last = time.monotonic()

    def add(self, table: str, rows: Iterable[tuple]) -> None:
        self.pending[table] += list(rows)

    def flush(self, force: bool = True) -> None:
        if not force and time.monotonic() - self.last < FLUSH_SECONDS:
            return
        for table, schema in self.schemas.items():
            rows, self.pending[table] = self.pending[table], []
            if rows:
                self.spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(
                    table_fqn(self.catalog, "control", table)
                )
        self.last = time.monotonic()


@dataclass(frozen=True)
class Progress:
    point: Point
    base: Base | None
    candidates: int
    changed: int
    selected: int
    left: int
    records: dict[tuple[str, ...], Record]

    @property
    def null(self) -> Record | None:
        return self.records.get(())

    @property
    def problems(self) -> list[str]:
        found = []
        if self.base is not None and not self.base.reproducible:
            found.append("its decision in the real world is not reproducible")
        if self.null is not None and not null_passed(self.null.effect):
            found.append("the null world changed it")
        broken = [" ".join(r.withheld) or NULL for r in self.records.values() if r.rebuilt is False]
        if broken:
            found.append(f"{len(broken)} worlds differ from the full rebuild: {broken[:3]}")
        return found

    @property
    def state(self) -> str:
        if self.problems:
            return FAILED
        return ATTRIBUTED if self.base is not None and not self.left else PARTIAL


def describe(progress: Progress) -> str:
    point, base = progress.point, progress.base
    if base is None:
        return f"{point.label}: no decision in the real world yet"
    settled = ", ".join(
        f"{k} {v}" if isinstance(v, str) else f"{k} {v:.4f}"
        for k, v in sorted(base.settled.items())
    )
    records = progress.records.values()
    statuses = {
        s: sum(1 for r in records if r.status == s and r.withheld)
        for s in (EVALUATED, SCREENED, SAMPLED)
    }
    lines = [
        f"{point.label} (point {point.key[:SHORT]}): settled {settled or 'nothing'}; "
        f"{progress.candidates:,} items knowable, {progress.changed:,} change the decider's "
        f"inputs, {progress.selected:,} of those to attribute; done {statuses[EVALUATED]:,} "
        f"evaluated, {statuses[SCREENED]:,} screened (inputs unchanged), {statuses[SAMPLED]:,} "
        f"sampled; {progress.left:,} worlds left"
    ]
    null = progress.null
    if null is not None:
        shift = max((abs(v) for v in null.effect.effects.values()), default=0.0)
        smallest = min(null.effect.p_values.values(), default=1.0)
        state = "changes nothing" if null_passed(null.effect) else "CHANGED THE DECISION"
        rebuilt = {None: "not checked", True: "equal", False: "DIFFERENT"}[null.rebuilt]
        lines.append(
            f"  null world {state}: outcomes {sorted(set(null.effect.outcomes.values()))}, "
            f"largest shift {shift:.4f}, smallest p {smallest:.3f}, "
            f"rebuild {rebuilt}"
        )
    caused = sorted(
        (r for r in records if r.withheld and r.effect.caused),
        key=lambda r: (-max(abs(r.effect.effects[p]) for p in r.effect.caused), r.withheld),
    )
    verified = sum(1 for r in records if r.rebuilt)
    lines.append(
        f"  {len(caused):,} items change at least one part; {verified:,} worlds equal the full "
        "rebuild"
    )
    for r in caused[:TOP]:
        parts = ", ".join(
            f"{p} {r.effect.outcomes[p]} {r.effect.effects[p]:+.4f}"
            + (f" p {r.effect.p_values[p]:.3f}" if p in r.effect.p_values else "")
            for p in r.effect.caused
        )
        lines.append(f"  {' '.join(r.withheld)}: {parts}")
    lines += [f"  problem: {p}" for p in progress.problems]
    return "\n".join(lines)


def attribute_point(
    spark,
    point: Point,
    loaded: Loaded,
    worlds: Worlds,
    base: Base | None,
    records: dict[tuple[str, ...], Record],
    complete: Complete | None,
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Progress:
    info = information_set(loaded.items, point.at)
    items = sorted(info.items.values(), key=lambda i: (i.knowable_at, i.id))
    records = dict(records)

    def progress(changed: int, selected: int, left: int) -> Progress:
        return Progress(point, base, len(items), changed, selected, left, records)

    def keep(found: Attributed) -> None:
        if found.effect.valid:
            records[found.withheld] = found.record(base)
        writer.add(ATTRIBUTIONS, [attribution_row(args, point, base, found, at)])
        if found.status == SAMPLED:
            writer.add(
                DECISION_SAMPLES,
                samples_of(args.run_id, found.world_id, point.decider, [found.samples], at),
            )
        writer.flush(force=False)

    def late() -> bool:
        return time.monotonic() > deadline

    if base is None:
        if late():
            return progress(0, 0, len(items) + 1)
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
        return progress(0, 0, 0)
    if () not in records and not late():
        keep(evaluate(point, base, worlds, None, complete, args))
    todo = [i for i in items if (i.id,) not in records]
    worlds.prepare(spark, [i.id for i in todo])
    if point.decider == STRATEGY_DECIDER:
        for item in todo:
            if late():
                break
            keep(evaluate(point, base, worlds, item, complete, args))
        changed = sum(1 for r in records.values() if r.withheld and r.inputs_changed)
        left = sum(1 for w in [(), *((i.id,) for i in items)] if w not in records)
        return progress(changed, len(items), left)
    changed = [i for i in items if (i.id,) in records and records[(i.id,)].inputs_changed]
    unscreened = 0
    for n, item in enumerate(todo):
        if late():
            unscreened = len(todo) - n
            break
        inputs = agent_inputs(point, base, worlds, item.id)
        if inputs[1] == base.input_hash:
            keep(evaluate(point, base, worlds, item, complete, args, inputs))
        else:
            changed.append(item)
    selected = select(changed, args.bars)
    for item in selected:
        if (item.id,) not in records and not late():
            keep(evaluate(point, base, worlds, item, complete, args))
    left = unscreened + sum(1 for w in [(), *((i.id,) for i in selected)] if w not in records)
    return progress(len(changed), len(selected), left)


def attribute_all(spark, args, at: datetime) -> list[Progress]:
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
    done: dict[str, dict] = {}
    for row in spark.sql(stored_rows_query(args.catalog, keys)).collect():
        done.setdefault(row["point_key"], {})[tuple(row["withheld"])] = stored_record(row)
    complete = endpoint_caller(args.endpoint) if AGENT_DECIDER in args.deciders else None
    worlds, writer = Worlds(loaded), Writer(spark, args.catalog, SCHEMAS)
    deadline = time.monotonic() + args.minutes * 60
    found = []
    for point in points:
        progress = attribute_point(
            spark,
            point,
            loaded,
            worlds,
            bases.get(point.key),
            done.get(point.key, {}),
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

    found = attribute_all(spark, args, datetime.now(UTC))
    failures = [f"{p.point.label}: {'; '.join(p.problems)}" for p in found if p.state == FAILED]
    if failures:
        raise AttributionFailed(failures)
    left = [p for p in found if p.state == PARTIAL]
    if left:
        print(
            f"{PARTIAL}: {len(left)} of {len(found)} decisions still have worlds to attribute; "
            "run the job again with the same parameters to continue"
        )
        return
    print(f"OK: {len(found)} decisions in {args.catalog} attributed, each null world unchanged")


if __name__ == "__main__":
    main()

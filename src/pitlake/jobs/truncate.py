import argparse
import re
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from pitlake.attribution import (
    EVALUATED,
    SAMPLED,
    SCREENED,
    Base,
    Effect,
    agent_effect,
    calls_of,
    inputs_hash,
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
    run_agent,
    strategy_sample,
    summarize,
)
from pitlake.horizon import (
    FULL,
    HORIZONS,
    REFINE,
    Curve,
    Measured,
    agreement,
    cutoff,
    kept_within,
    ladder,
    measure,
    ordered,
    rank,
    screened,
    span,
    trace,
)
from pitlake.information import Item, information_set
from pitlake.jobs.attribute import (
    Point,
    Writer,
    keys_in,
    make_point,
    new_base,
    stored_base,
    stored_points_query,
)
from pitlake.jobs.decide import draw, endpoint_caller, samples_of
from pitlake.jobs.information_set import SHORT, Loaded, decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.jobs.suffice import Builder, Late, ablation_of, parts_text
from pitlake.sufficiency import by_kind, kept_hash, order
from pitlake.tables import (
    ABLATION_CHANGES,
    ABLATION_CHANGES_SCHEMA,
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    DECISION_SAMPLES_SCHEMA,
    HORIZON_CURVES,
    HORIZON_CURVES_SCHEMA,
    HORIZON_TESTS,
    HORIZON_TESTS_SCHEMA,
)

MEASURED = "MEASURED"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
HOLDS = "holds"
BREAKS = "breaks"
VERIFY_ALL = "all"
VERIFY_BOUNDARY = "boundary"
VERIFY = (VERIFY_ALL, VERIFY_BOUNDARY)
SCHEMAS = {
    DECISION_SAMPLES: DECISION_SAMPLES_SCHEMA,
    ATTRIBUTION_POINTS: ATTRIBUTION_POINTS_SCHEMA,
    HORIZON_TESTS: HORIZON_TESTS_SCHEMA,
    ABLATION_CHANGES: ABLATION_CHANGES_SCHEMA,
    ABLATIONS: ABLATIONS_SCHEMA,
    HORIZON_CURVES: HORIZON_CURVES_SCHEMA,
}


class HorizonFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} decisions failed the horizon: {'; '.join(failures)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", default="", help="Chat endpoint the agent decides with")
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--deciders", default=" ".join(DECIDERS))
    parser.add_argument("--samples", default="10", help="Agent samples per world")
    parser.add_argument("--min-agreement", default="0.8")
    parser.add_argument("--horizons", default=" ".join(map(str, HORIZONS)), help="Days kept")
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
    args.samples, args.workers = (int(v.strip() or 0) for v in (args.samples, args.workers))
    if args.samples < 1 or args.workers < 1:
        parser.error("--samples and --workers must be at least 1")
    args.min_agreement = float(args.min_agreement.strip() or 0)
    if not 0.5 < args.min_agreement <= 1:
        parser.error("--min-agreement is a share above 0.5 and at most 1")
    try:
        args.horizons = ladder(int(d) for d in re.split(r"[\s,;]+", args.horizons) if d)
    except ValueError as error:
        parser.error(f"--horizons: {error}")
    args.minutes = float(args.minutes.strip() or 0)
    return args


def ladder_text(horizons: Iterable[int]) -> str:
    return " ".join(map(str, horizons))


@dataclass(frozen=True)
class Tried:
    test_id: str
    kept: int
    measured: Measured
    valid: bool
    status: str
    input_hash: str
    rebuilt: bool | None = None


@dataclass(frozen=True)
class Probed:
    days: int | None
    phase: str
    kept: frozenset[str]
    tried: Tried
    samples: list[Sample]
    errors: int
    decision: str | None
    effect: Effect
    calls: tuple[tuple[str, str], ...]
    removed: int
    recomputed: int
    seconds: float


def world_row(args, point: Point, test: Probed, at: datetime) -> tuple:
    tried, effect, measured = test.tried, test.effect, test.tried.measured
    return (
        tried.test_id,
        point.key,
        args.run_id,
        at,
        test.phase,
        test.days,
        cutoff(point.at, test.days),
        kept_hash(test.kept),
        len(test.kept),
        by_kind(test.kept),
        tried.status,
        measured.reproduced,
        tried.valid,
        len(test.samples),
        sum(1 for s in test.samples if not s.valid),
        test.errors,
        test.decision,
        measured.same,
        measured.agreement,
        measured.outcomes,
        effect.effects,
        effect.p_values,
        [canonical(list(c)) for c in test.calls],
        tried.input_hash,
        test.removed,
        test.recomputed,
        tried.rebuilt,
        test.seconds,
        args.code_commit,
        package_version(),
    )


class Prober:
    def __init__(
        self,
        spark,
        point: Point,
        base: Base,
        builder: Builder,
        items: list[Item],
        complete: Complete | None,
        args,
        writer: Writer,
        deadline: float,
        at: datetime,
        tried: dict[str, Tried],
    ):
        self.spark, self.point, self.base, self.builder = spark, point, base, builder
        self.items, self.complete, self.args, self.writer = items, complete, args, writer
        self.deadline, self.at = deadline, at
        self.tried = dict(tried)
        self.earlier = len(self.tried)
        self.steps: dict[int | None, Tried] = {}
        self.broken = sum(1 for t in self.tried.values() if t.rebuilt is False)

    def kept(self, days: int | None) -> frozenset[str]:
        return kept_within(self.items, self.point.at, days)

    def __call__(self, days: int | None, phase: str) -> Measured:
        kept = self.kept(days)
        found = self.tried.get(kept_hash(kept))
        if found is None:
            if time.monotonic() > self.deadline:
                raise Late
            found = self.test(kept, days, phase)
        self.steps[days] = found
        return found.measured

    def test(self, kept: frozenset[str], days: int | None, phase: str) -> Tried:
        started = time.perf_counter()
        point, base, builder = self.point, self.base, self.builder
        world = builder.build(self.spark, kept)
        samples, errors, decision, calls = [], 0, None, ()
        if point.decider == STRATEGY_DECIDER:
            rows = builder.rows(world, STRATEGY_REACH)
            sample = strategy_sample(rows, point.times, point.params)
            samples, decision, seen = [sample], sample.decision, sample.input_hash
            effect = strategy_effect(base, sample.decision)
            measured, status = measure(effect), EVALUATED
        else:
            tools = agent_tools(View(builder.rows(world, AGENT_REACH), point.at))
            seen = inputs_hash(tools, base.calls)
            if seen == base.input_hash:
                effect, status, calls = unchanged(base), SCREENED, base.calls
                measured = measure(effect, screened(base))
            else:
                samples = draw(
                    lambda: run_agent(self.complete, tools, point.at),
                    1,
                    point.samples,
                    self.args.workers,
                )[0]
                errors = sum(1 for s in samples if not s.valid and not s.model_calls)
                effect = agent_effect(base, samples, point.minimum)
                decision = summarize([samples], point.minimum).decision
                calls = calls_of(samples)
                seen = inputs_hash(tools, calls)
                measured, status = measure(effect, agreement(base, samples)), SAMPLED
        rebuilt = builder.rebuilt(world) if self.args.verify == VERIFY_ALL else None
        tried = Tried(uuid.uuid4().hex, len(kept), measured, not errors, status, seen, rebuilt)
        test = Probed(
            days,
            phase,
            kept,
            tried,
            samples,
            errors,
            decision,
            effect,
            calls,
            len(world.removal.removed),
            len(world.added),
            time.perf_counter() - started,
        )
        self.writer.add(HORIZON_TESTS, [world_row(self.args, point, test, self.at)])
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


@dataclass(frozen=True)
class Step:
    days: int | None
    phase: str
    tried: Tried
    boundary: str | None = None
    ablation_id: str | None = None
    coherent: bool | None = None


def curve_rows(args, point: Point, curve: Curve, steps: list[Step], at: datetime) -> list[tuple]:
    return [
        (
            point.key,
            args.run_id,
            at,
            point.decider,
            point.at,
            ladder_text(args.horizons),
            step.days,
            step.phase,
            cutoff(point.at, step.days),
            step.tried.kept,
            step.tried.test_id,
            step.tried.measured.reproduced,
            step.tried.measured.same,
            step.tried.measured.agreement,
            step.tried.measured.outcomes,
            step.tried.rebuilt,
            curve.holds_to,
            curve.breaks_at,
            step.boundary,
            step.ablation_id,
            step.coherent,
            args.code_commit,
            package_version(),
        )
        for step in steps
    ]


def stored_tests_query(catalog: str, keys: Iterable[str]) -> str:
    return (
        "SELECT point_key, test_id, kept_hash, kept_count, reproduced, same, agreement, "
        "outcomes, valid, status, input_hash, rebuilt "
        f"FROM {table_fqn(catalog, 'control', HORIZON_TESTS)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND valid ORDER BY created_at"
    )


def stored_curves_query(catalog: str, keys: Iterable[str], horizons: Iterable[int]) -> str:
    return (
        "SELECT point_key, run_id, horizon_days, phase, kept_count, test_id, reproduced, same, "
        "agreement, outcomes, rebuilt, holds_to, breaks_at, boundary, ablation_id, coherent "
        f"FROM {table_fqn(catalog, 'control', HORIZON_CURVES)} "
        f"WHERE point_key IN ({keys_in(keys)}) AND ladder = '{ladder_text(horizons)}' "
        "ORDER BY created_at"
    )


def stored_measured(row) -> Measured:
    return Measured(row["reproduced"], row["same"], row["agreement"], dict(row["outcomes"]))


def stored_tried(row) -> Tried:
    return Tried(
        row["test_id"],
        row["kept_count"],
        stored_measured(row),
        row["valid"],
        row["status"],
        row["input_hash"],
        row["rebuilt"],
    )


@dataclass(frozen=True)
class Stored:
    curve: Curve
    steps: tuple[Step, ...]


def stored_curves(rows: Iterable) -> dict[str, Stored]:
    by_point: dict[str, list] = {}
    for row in rows:
        by_point.setdefault(row["point_key"], []).append(row)
    found = {}
    for key, every in by_point.items():
        last = every[-1]["run_id"]
        mine = [r for r in every if r["run_id"] == last]
        steps = tuple(
            Step(
                r["horizon_days"],
                r["phase"],
                Tried(
                    r["test_id"], r["kept_count"], stored_measured(r), True, "", "", r["rebuilt"]
                ),
                r["boundary"],
                r["ablation_id"],
                r["coherent"],
            )
            for r in mine
        )
        points = ordered((s.days, s.phase, s.tried.measured) for s in steps)
        curve = Curve(points, mine[0]["holds_to"], mine[0]["breaks_at"])
        found[key] = Stored(curve, tuple(sorted(steps, key=lambda s: rank(s.days))))
    return found


@dataclass(frozen=True)
class Progress:
    point: Point
    base: Base | None
    candidates: int
    tests: int
    earlier: int
    sampled: int
    broken: int
    curve: Curve | None = None
    steps: tuple[Step, ...] = ()
    errors: int = 0

    @property
    def problems(self) -> list[str]:
        found = []
        if self.base is not None and not self.base.reproducible:
            found.append("its decision in the real world is not reproducible")
        if self.curve is not None and not self.curve.full.reproduced:
            found.append("the untruncated world does not decide the same")
        if self.broken:
            found.append(f"{self.broken} worlds differ from the full rebuild")
        for step in self.steps:
            if step.coherent is False:
                found.append(f"the {step.boundary} ablation {step.ablation_id} is not coherent")
        return found

    @property
    def state(self) -> str:
        if self.problems:
            return FAILED
        return MEASURED if self.curve is not None else PARTIAL


def label(days: int | None) -> str:
    if days is None:
        return FULL
    return f"{days} day{'' if days == 1 else 's'}"


def truncation(curve: Curve) -> str:
    if not curve.full.reproduced:
        return "  the untruncated world decides differently"
    if curve.holds_to is None:
        return "  reproducible only untruncated"
    if curve.breaks_at is None:
        return f"  reproducible at every horizon down to {label(curve.holds_to)}"
    text = f"  reproducible down to {label(curve.holds_to)}, not at {label(curve.breaks_at)}"
    if curve.recovers:
        text += f"; reproducible again at {', '.join(map(str, curve.recovers))} days"
    return text


def describe(progress: Progress) -> str:
    point, base, curve = progress.point, progress.base, progress.curve
    if base is None:
        return f"{point.label}: no decision in the real world yet"
    head = (
        f"{point.label} (point {point.key[:SHORT]}): settled {parts_text(base) or 'nothing'}; "
        f"{progress.candidates:,} items knowable; {progress.tests:,} worlds decided "
        f"({progress.earlier:,} from earlier runs, {progress.sampled:,} sampled)"
    )
    if curve is None:
        lines = [f"{head}; stopped before the curve was complete"]
        if progress.errors:
            lines[0] += (
                f"; the endpoint failed in {progress.errors:,} worlds, which the next run "
                "decides again"
            )
    else:
        lines = [head, truncation(curve)]
        for step in progress.steps:
            m = step.tried.measured
            mark = "same" if m.reproduced else "DIFFERS"
            text = (
                f"  {label(step.days):>9} {'refined' if step.phase == REFINE else '':<7} "
                f"{step.tried.kept:>6,} items  "
                f"{mark:<7} parts same {m.same:.2f}, agreement {m.agreement:.2f}"
            )
            if step.boundary:
                coherent = {None: "not checked", True: "coherent", False: "INCOHERENT"}
                text += f"; {step.boundary}: ablation {step.ablation_id} {coherent[step.coherent]}"
            lines.append(text)
        held = ", ".join(
            f"{part} {'nothing cut' if days is None else days}"
            for part, days in sorted(curve.parts().items())
        )
        lines.append(f"  each part holds down to (days): {held}")
    lines += [f"  problem: {p}" for p in progress.problems]
    return "\n".join(lines)


def summary(found: list[Progress], horizons: Iterable[int]) -> list[str]:
    lines = []
    for decider in DECIDERS:
        mine = [p for p in found if p.point.decider == decider and p.state == MEASURED]
        if not mine:
            continue
        held = ", ".join(label(p.curve.holds_to) for p in mine)
        lines.append(f"CURVE: {decider} over {len(mine)} decisions, reproducible down to {held}")
        for days in (None, *horizons):
            at = [m for p in mine for d, _, m in p.curve.points if d == days]
            if not at:
                continue
            kept = sum(1 for m in at if m.reproduced)
            same = sum(m.same for m in at) / len(at)
            agree = sum(m.agreement for m in at) / len(at)
            lines.append(
                f"  {label(days):>9}: {kept} of {len(at)} reproduced, parts same {same:.2f}, "
                f"agreement {agree:.2f}"
            )
    return lines


def truncate_point(
    spark,
    point: Point,
    loaded: Loaded,
    builder: Builder,
    base: Base | None,
    tried: dict[str, Tried],
    stored: Stored | None,
    complete: Complete | None,
    args,
    writer: Writer,
    deadline: float,
    at: datetime,
) -> Progress:
    info = information_set(loaded.items, point.at)
    items: list[Item] = sorted(info.items.values(), key=order)
    sampled = sum(1 for t in tried.values() if t.status == SAMPLED)
    broken = sum(1 for t in tried.values() if t.rebuilt is False)

    def late() -> bool:
        return time.monotonic() > deadline

    def progress(prober: Prober | None = None) -> Progress:
        if prober is None:
            return Progress(point, base, len(items), len(tried), len(tried), sampled, broken)
        return Progress(
            point,
            base,
            len(items),
            len(prober.tried),
            prober.earlier,
            prober.sampled,
            prober.broken,
            errors=sum(1 for t in prober.steps.values() if not t.valid),
        )

    if stored is not None:
        return Progress(
            point,
            base,
            len(items),
            len(tried),
            len(tried),
            sampled,
            broken,
            stored.curve,
            stored.steps,
        )
    if base is None:
        if late():
            return progress()
        base = new_base(point, loaded, info, complete, args, writer, at)
    if not base.reproducible:
        return progress()
    prober = Prober(spark, point, base, builder, items, complete, args, writer, deadline, at, tried)
    try:
        curve = trace(args.horizons, prober, span(items, point.at))
    except Late:
        return progress(prober)
    if late() or not all(t.valid for t in prober.steps.values()):
        return progress(prober)
    pairs = ((curve.holds_to, HOLDS), (curve.breaks_at, BREAKS))
    boundaries = {d: b for d, b in pairs if d is not None and curve.full.reproduced}
    steps = []
    for days, phase, _ in curve.points:
        boundary, ablation_id, coherent = boundaries.get(days), None, None
        if boundary is not None:
            kept = prober.kept(days)
            withheld = tuple(i for i in builder.known if i not in kept)
            ablation_id, coherent = ablation_of(spark, point, loaded, withheld, args, writer, at)
        steps.append(Step(days, phase, prober.steps[days], boundary, ablation_id, coherent))
    writer.add(HORIZON_CURVES, curve_rows(args, point, curve, steps, at))
    writer.flush()
    found = progress(prober)
    return Progress(
        point,
        base,
        found.candidates,
        found.tests,
        found.earlier,
        found.sampled,
        found.broken,
        curve,
        tuple(steps),
    )


def truncate_all(spark, args, at: datetime) -> list[Progress]:
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
    curves = stored_curves(
        spark.sql(stored_curves_query(args.catalog, keys, args.horizons)).collect()
    )
    complete = endpoint_caller(args.endpoint) if AGENT_DECIDER in args.deciders else None
    builder, writer = Builder(loaded), Writer(spark, args.catalog, SCHEMAS)
    deadline = time.monotonic() + args.minutes * 60
    found = []
    for point in points:
        progress = truncate_point(
            spark,
            point,
            loaded,
            builder,
            bases.get(point.key),
            tried.get(point.key, {}),
            curves.get(point.key),
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

    found = truncate_all(spark, args, datetime.now(UTC))
    for line in summary(found, args.horizons):
        print(line)
    failures = [f"{p.point.label}: {'; '.join(p.problems)}" for p in found if p.state == FAILED]
    if failures:
        raise HorizonFailed(failures)
    left = [p for p in found if p.state == PARTIAL]
    if left:
        print(
            f"{PARTIAL}: {len(left)} of {len(found)} decisions still have worlds to decide; run "
            "the job again with the same parameters to continue"
        )
        return
    print(
        f"OK: {len(found)} decisions in {args.catalog} have a degradation curve and a "
        "truncation point, each reproducible untruncated"
    )


if __name__ == "__main__":
    main()

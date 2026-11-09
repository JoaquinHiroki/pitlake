import argparse
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from pitlake.ablation import (
    COHERENT,
    MODES,
    apply,
    differences,
    hidden,
    owned_left,
    plan,
    rebuild,
    recompute,
    sample_items,
    without_bars,
)
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.information import Accounting, account, information_set, parse_item_id, sessions
from pitlake.jobs.information_set import (
    REACH,
    SHORT,
    Loaded,
    decisions_query,
    group_decisions,
    load,
    rebuilt_inputs,
)
from pitlake.jobs.research import fingerprint_inputs, package_version
from pitlake.tables import (
    ABLATION_CHANGES,
    ABLATION_CHANGES_SCHEMA,
    ABLATIONS,
    ABLATIONS_SCHEMA,
)

REMOVED = "removed"
ADDED = "added"


class AblationIncoherent(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        detail = "; ".join(failures[:10])
        super().__init__(f"{len(failures)} ablated worlds are not coherent: {detail}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--backtest", default="")
    parser.add_argument("--withhold", default="", help="Item IDs withheld together, by spaces")
    parser.add_argument("--sample", default="0", help="Read items per kind to withhold one by one")
    parser.add_argument("--mode", default=COHERENT, choices=MODES)
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.backtest = args.backtest.strip()
    if args.backtest:
        validate_segment(args.backtest)
    named = [parse_item_id(i) for i in re.split(r"[\s;]+", args.withhold) if i]
    args.withhold = tuple(dict.fromkeys(named))
    args.sample = int(args.sample.strip() or 0)
    if args.sample < 0:
        parser.error("--sample must not be negative")
    return args


@dataclass(frozen=True)
class Outcome:
    ablation_id: str
    withheld: tuple[str, ...]
    removed: tuple[tuple, ...]
    recomputed: tuple[tuple, ...]
    world_rows: int
    extra: int
    missing: int
    sample: tuple[str, ...]
    owned: int
    accounting: Accounting
    set_hash: str
    input_hash: str
    seconds: tuple[float, float, float]

    @property
    def rebuilt(self) -> bool:
        return self.extra == 0 and self.missing == 0

    def coherent(self, recorded: str) -> bool:
        unchanged = bool(self.withheld) or self.input_hash == recorded
        return self.rebuilt and self.owned == 0 and self.accounting.unaccounted == 0 and unchanged


def ablate(spark, loaded: Loaded, withheld: tuple[str, ...], mode: str) -> Outcome:
    days = sessions(loaded.items)
    remaining = information_set(loaded.items, loaded.times[-1], withheld)
    started = time.perf_counter()
    if mode == COHERENT:
        removal = plan(loaded.rows, withheld, days)
        recomputed = recompute(loaded.bars, removal, REACH)
    else:
        removal, recomputed = hidden(loaded.rows, withheld), []
    world = apply(loaded.rows, removal, recomputed)
    removed_at = time.perf_counter()
    received = rebuilt_inputs(spark, world, loaded.times)
    _, input_hash = fingerprint_inputs(received)
    read_at = time.perf_counter()
    reference = rebuild(loaded.bars, loaded.fundamentals, loaded.features, withheld, REACH)
    rebuilt_at = time.perf_counter()
    extra, missing, sample = differences(world, reference)
    accounting = account(remaining, world, received, without_bars(days, withheld))
    return Outcome(
        uuid.uuid4().hex,
        withheld,
        removal.removed,
        tuple(recomputed),
        len(world),
        extra,
        missing,
        tuple(f"{side}: {row}" for side, row in sample),
        owned_left(world, withheld),
        accounting,
        remaining.hash,
        input_hash,
        (removed_at - started, read_at - removed_at, rebuilt_at - read_at),
    )


def ablation_row(
    args, outcome: Outcome, records: list, loaded: Loaded, base_hash: str, at: datetime
) -> tuple:
    first = records[0]
    recorded = first["input_hash"]
    return (
        outcome.ablation_id,
        args.run_id,
        at,
        first["research_run_id"],
        [r["backtest_id"] for r in records],
        loaded.times[-1],
        first["period_start"],
        first["period_end"],
        dict(loaded.versions),
        args.mode,
        list(outcome.withheld),
        base_hash,
        outcome.set_hash,
        len(outcome.removed),
        len(outcome.recomputed),
        outcome.world_rows,
        outcome.extra,
        outcome.missing,
        list(outcome.sample),
        outcome.owned,
        outcome.accounting.unaccounted,
        list(outcome.accounting.sample),
        recorded,
        outcome.input_hash,
        outcome.input_hash != recorded,
        outcome.coherent(recorded),
        *outcome.seconds,
        args.code_commit,
        package_version(),
    )


def change_rows(outcome: Outcome) -> list[tuple]:
    return [
        (outcome.ablation_id, change, *row)
        for change, rows in ((REMOVED, outcome.removed), (ADDED, outcome.recomputed))
        for row in rows
    ]


def describe(outcome: Outcome, recorded: str) -> str:
    what = " ".join(outcome.withheld) or "nothing (null world)"
    changed = "changed" if outcome.input_hash != recorded else "unchanged"
    rebuilt = "matches" if outcome.rebuilt else f"+{outcome.extra}/-{outcome.missing} against"
    removal, inputs, rebuild_seconds = outcome.seconds
    return (
        f"withheld {what}: removed {len(outcome.removed)} rows, recomputed "
        f"{len(outcome.recomputed)}; inputs {outcome.input_hash[:SHORT]} {changed}; "
        f"{rebuilt} the full rebuild; {outcome.owned} owned left; "
        f"{outcome.accounting.unaccounted} unaccounted; {removal:.1f} s removal, "
        f"{inputs:.1f} s inputs, {rebuild_seconds:.1f} s rebuild"
    )


def ablate_sets(spark, args, at: datetime) -> tuple[list[tuple], list[tuple], list[str]]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'} to ablate")
    ablations, changes, failures = [], [], []
    for decision, backtests in group_decisions(records).items():
        loaded = load(spark, args.catalog, decision)
        base = information_set(loaded.items, loaded.times[-1])
        recorded = backtests[0]["input_hash"]
        print(
            f"{decision.period_start} to {decision.period_end}, set {base.hash[:SHORT]}: "
            f"{len(base.items):,} items, {len(loaded.rows):,} Gold rows, "
            f"recorded input {recorded[:SHORT]}"
        )
        null = ablate(spark, loaded, (), COHERENT)
        chosen = [args.withhold] if args.withhold else []
        chosen += [(i,) for i in sample_items(loaded.items, null.accounting.read, args.sample)]
        for outcome in [null, *(ablate(spark, loaded, w, args.mode) for w in chosen)]:
            ablations.append(ablation_row(args, outcome, backtests, loaded, base.hash, at))
            changes += change_rows(outcome)
            coherent = outcome.coherent(recorded)
            print(f"{'COHERENT' if coherent else 'INCOHERENT'}: {describe(outcome, recorded)}")
            for line in (*outcome.sample, *outcome.accounting.sample[:5]):
                print(f"  {line}")
            if not coherent:
                failures.append(" ".join(outcome.withheld) or "null world")
    return ablations, changes, failures


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)

    ablations, changes, failures = ablate_sets(spark, args, at)
    if changes:
        spark.createDataFrame(changes, schema=ABLATION_CHANGES_SCHEMA).write.mode(
            "append"
        ).saveAsTable(table_fqn(args.catalog, "control", ABLATION_CHANGES))
    spark.createDataFrame(ablations, schema=ABLATIONS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", ABLATIONS)
    )
    if failures:
        raise AblationIncoherent(failures)
    print(f"OK: {len(ablations)} ablated worlds in {args.catalog}, every one coherent")


if __name__ == "__main__":
    main()

import argparse
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime

from pitlake.backtest import FEATURES, UNIVERSE, decision_times
from pitlake.bronze import sql_string
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.gold import GLOBAL_ENTITY
from pitlake.information import (
    WORLD_SCHEMA,
    Accounting,
    InformationSet,
    Item,
    Reach,
    account,
    enumerate_items,
    feature_rows,
    in_reach,
    information_set,
    parse_item_id,
    sessions,
    world,
)
from pitlake.jobs.research import (
    FEATURES_KEY,
    fingerprint_inputs,
    originals_query,
    package_version,
    read_inputs,
)
from pitlake.snapshots import readable_version
from pitlake.tables import (
    BACKTEST_RUNS,
    BARS_1D,
    FUNDAMENTALS,
    GOLD_COMMITS,
    INFORMATION_ITEMS,
    INFORMATION_ITEMS_SCHEMA,
    INFORMATION_SETS,
    INFORMATION_SETS_SCHEMA,
)
from pitlake.tables import FEATURES as FEATURES_TABLE

REACH = Reach((*(a.entity for a in UNIVERSE), GLOBAL_ENTITY), FEATURES)
BARS_KEY = f"gold.{BARS_1D}"
FUNDAMENTALS_KEY = f"gold.{FUNDAMENTALS}"
SHORT = 12


class InformationIncomplete(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(
            f"{len(failures)} information sets are not complete: {'; '.join(failures)}"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--backtest", default="", help="A backtest ID or research run ID; default the latest run"
    )
    parser.add_argument("--omit", default="", help="An item ID to leave out, as a drill")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.backtest = args.backtest.strip()
    if args.backtest:
        validate_segment(args.backtest)
    args.omit = (parse_item_id(args.omit),) if args.omit.strip() else ()
    return args


def decisions_query(catalog: str, backtest: str) -> str:
    if backtest:
        return originals_query(catalog, backtest)
    table = table_fqn(catalog, "control", BACKTEST_RUNS)
    return (
        f"SELECT * FROM {table} WHERE replay_of IS NULL AND research_run_id = ("
        f"SELECT research_run_id FROM {table} WHERE replay_of IS NULL "
        "ORDER BY created_at DESC LIMIT 1) ORDER BY created_at, backtest_id"
    )


def gold_commit_query(catalog: str, run_id: str) -> str:
    return (
        "SELECT gold_table, gold_version "
        f"FROM {table_fqn(catalog, 'control', GOLD_COMMITS)} WHERE run_id = {sql_string(run_id)}"
    )


def version_query(catalog: str, table: str, version: int) -> str:
    return f"SELECT * FROM {table_fqn(catalog, 'gold', table)} VERSION AS OF {int(version)}"


@dataclass(frozen=True)
class Decision:
    commit_run_id: str
    features_version: int
    period_start: date
    period_end: date


def group_decisions(records: list) -> dict[Decision, list]:
    groups: dict[Decision, list] = {}
    for r in records:
        key = Decision(
            r["gold_commit_run_id"],
            int(r["table_versions"][FEATURES_KEY]),
            r["period_start"],
            r["period_end"],
        )
        groups.setdefault(key, []).append(r)
    return groups


def gold_versions(spark, catalog: str, decision: Decision) -> dict[str, int]:
    commits = {
        r["gold_table"]: r["gold_version"]
        for r in spark.sql(gold_commit_query(catalog, decision.commit_run_id)).collect()
    }
    missing = sorted({BARS_1D, FUNDAMENTALS} - set(commits))
    if missing:
        raise RuntimeError(f"Gold commit {decision.commit_run_id} has no version of {missing}")
    return {
        FEATURES_KEY: decision.features_version,
        BARS_KEY: readable_version(spark, table_fqn(catalog, "gold", BARS_1D), commits[BARS_1D]),
        FUNDAMENTALS_KEY: readable_version(
            spark, table_fqn(catalog, "gold", FUNDAMENTALS), commits[FUNDAMENTALS]
        ),
    }


def set_row(
    args,
    enumeration_id: str,
    record,
    info: InformationSet,
    accounting: Accounting,
    versions: dict[str, int],
    world_rows: int,
    rebuilt: str,
    at: datetime,
) -> tuple:
    reproduced = rebuilt == record["input_hash"]
    return (
        enumeration_id,
        args.run_id,
        at,
        record["backtest_id"],
        record["research_run_id"],
        info.at,
        record["period_start"],
        record["period_end"],
        dict(versions),
        info.hash,
        len(info.items),
        info.by_kind(),
        len(accounting.read),
        len(info.items) - len(accounting.read),
        info.later,
        world_rows,
        accounting.values,
        list(info.omitted),
        accounting.unaccounted,
        list(accounting.sample),
        record["input_hash"],
        rebuilt,
        reproduced,
        reproduced and accounting.unaccounted == 0,
        args.code_commit,
        package_version(),
    )


def item_rows(
    args, enumeration_id: str, info: InformationSet, accounting: Accounting, at: datetime
) -> list[tuple]:
    return [
        (
            enumeration_id,
            item.id,
            item.kind,
            item.entity,
            item.key,
            item.knowable_at,
            item.digest,
            accounting.owned.get(item.id, 0),
            accounting.status(item.id),
            args.run_id,
            at,
        )
        for item in sorted(info.items.values(), key=lambda i: i.id)
    ]


def summary(info: InformationSet, accounting: Accounting, rebuilt: str, recorded: str) -> str:
    kinds = ", ".join(f"{k} {n:,}" for k, n in info.by_kind().items())
    state = "IDENTICAL" if rebuilt == recorded else "DIFFERENT"
    return (
        f"{len(info.items):,} items knowable at {info.at:%Y-%m-%d %H:%M} UTC ({kinds}), "
        f"{len(accounting.read):,} read, {info.later:,} later; "
        f"{accounting.values:,} values rebuilt {rebuilt[:SHORT]} against recorded "
        f"{recorded[:SHORT]}: {state}; {accounting.unaccounted} unaccounted"
    )


def enumerate_decision(spark, catalog: str, decision: Decision, omit: tuple[str, ...]):
    versions = gold_versions(spark, catalog, decision)
    features = spark.sql(version_query(catalog, FEATURES_TABLE, versions[FEATURES_KEY]))
    bars = spark.sql(version_query(catalog, BARS_1D, versions[BARS_KEY]))
    fundamentals = spark.sql(version_query(catalog, FUNDAMENTALS, versions[FUNDAMENTALS_KEY]))
    items: list[Item] = enumerate_items(bars, fundamentals, features, REACH)
    times = decision_times(decision.period_start, decision.period_end)
    info = information_set(items, times[-1], omit)
    rows = feature_rows(in_reach(features, REACH))
    kept = world(rows, info)
    received = read_inputs(spark, spark.createDataFrame(kept, WORLD_SCHEMA), times)
    _, rebuilt = fingerprint_inputs(received)
    accounting = account(info, rows, received, sessions(items))
    return versions, info, accounting, len(kept), rebuilt


def enumerate_sets(spark, args, at: datetime) -> tuple[list[tuple], list[tuple], list[str]]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'} to enumerate")
    sets, items, failures = [], [], []
    for decision, backtests in group_decisions(records).items():
        versions, info, accounting, kept, rebuilt = enumerate_decision(
            spark, args.catalog, decision, args.omit
        )
        enumeration_id = uuid.uuid4().hex
        items += item_rows(args, enumeration_id, info, accounting, at)
        print(
            f"gold.features v{versions[FEATURES_KEY]}, gold.bars_1d v{versions[BARS_KEY]}, "
            f"gold.fundamentals v{versions[FUNDAMENTALS_KEY]}, {decision.period_start} to "
            f"{decision.period_end}: set {info.hash[:SHORT]}, {kept:,} Gold rows in its world"
        )
        for problem in accounting.sample:
            print(f"  unaccounted: {problem}")
        for record in backtests:
            sets.append(
                set_row(args, enumeration_id, record, info, accounting, versions, kept, rebuilt, at)
            )
            complete = rebuilt == record["input_hash"] and accounting.unaccounted == 0
            print(
                f"{'COMPLETE' if complete else 'INCOMPLETE'}: {record['backtest_id']} "
                f"{summary(info, accounting, rebuilt, record['input_hash'])}"
            )
            if not complete:
                failures.append(record["backtest_id"])
    return sets, items, failures


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)

    sets, items, failures = enumerate_sets(spark, args, at)
    spark.createDataFrame(items, schema=INFORMATION_ITEMS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", INFORMATION_ITEMS)
    )
    spark.createDataFrame(sets, schema=INFORMATION_SETS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", INFORMATION_SETS)
    )
    if failures:
        raise InformationIncomplete(failures)
    print(f"OK: {len(sets)} decisions in {args.catalog} rebuilt from their information sets alone")


if __name__ == "__main__":
    main()

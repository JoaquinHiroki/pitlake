import argparse
import shutil
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

from pitlake.bronze import parse_bool, parse_optional_date, sql_string
from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.gate import (
    PAIRS,
    THRESHOLDS,
    DayFiles,
    GateFailure,
    Result,
    VenuePair,
    days_to_compare,
    judge,
    rejection_measurements,
    venue_measurements,
)
from pitlake.quality import FAULTS, NO_FAULT, VENUE_FAULTS
from pitlake.tables import (
    CROSS_VENUE,
    CROSS_VENUE_LOG,
    CROSS_VENUE_LOG_SCHEMA,
    QUALITY_GATE_RESULTS,
    QUALITY_GATE_RESULTS_SCHEMA,
    QUALITY_WAIVERS,
    SILVER_COMMITS,
    SILVER_LOAD_LOG,
)
from pitlake.venues import compare, comparison_rows, daily_counts, minute_bars, minute_candles


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--period-start", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument("--reprocess", default="false")
    parser.add_argument("--inject-fault", default=NO_FAULT, choices=FAULTS)
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    start, end = parse_optional_date(args.period_start), parse_optional_date(args.period_end)
    args.fault = args.inject_fault if args.inject_fault in VENUE_FAULTS else NO_FAULT
    recompare = parse_bool(args.reprocess) or args.fault != NO_FAULT
    if recompare and (start is None or end is None):
        parser.error("--reprocess and venue faults require --period-start and --period-end")
    args.period = (start, end) if recompare else None
    return args


def uncommitted_log_query(catalog: str) -> str:
    last_commit = (
        "SELECT coalesce(max(committed_at), TIMESTAMP'1900-01-01 00:00:00') "
        f"FROM {table_fqn(catalog, 'control', SILVER_COMMITS)}"
    )
    return (
        "SELECT silver_table, period_start, rows_in, rejected_by_rule "
        f"FROM {table_fqn(catalog, 'control', SILVER_LOAD_LOG)} "
        f"WHERE processed_at > ({last_commit}) "
        "QUALIFY row_number() OVER (PARTITION BY silver_table, source_file "
        "ORDER BY processed_at DESC) = 1"
    )


def latest_files_query(catalog: str, table: str, symbol: str) -> str:
    return (
        "SELECT period_start, source_file, sha256 "
        f"FROM {table_fqn(catalog, 'control', SILVER_LOAD_LOG)} "
        f"WHERE silver_table = {sql_string(table)} AND symbol = {sql_string(symbol)} "
        "QUALIFY row_number() OVER (PARTITION BY source_file ORDER BY processed_at DESC) = 1"
    )


def day_predicate(pair: VenuePair, days: list[date]) -> str:
    listed = ", ".join(f"DATE'{d.isoformat()}'" for d in days)
    return f"pair = {sql_string(pair.name)} AND day IN ({listed})"


def result_rows(results: list[Result], run_id: str, at: datetime, fault: str) -> list[tuple]:
    return [
        (
            run_id,
            at,
            r.check,
            r.subject,
            r.day,
            r.measured,
            r.failed,
            float(r.allowed),
            r.passed,
            r.waived,
            r.waiver_reason,
            fault,
        )
        for r in results
    ]


def log_rows(pair: VenuePair, days: list[DayFiles], counts: dict, run_id: str, at: datetime):
    return [
        (
            pair.name,
            d.day,
            d.left_file,
            d.left_sha256,
            d.right_file,
            d.right_sha256,
            *counts[d.day],
            at,
            run_id,
        )
        for d in days
    ]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    at = datetime.now(UTC)

    log = spark.sql(uncommitted_log_query(args.catalog)).collect()
    measurements = rejection_measurements(tuple(r) for r in log)
    print(f"rejection_rate: {len(log)} files transformed since the last Silver commit")

    staging_root = Path(volume_path(args.catalog, "silver", STAGING_VOLUME)) / args.run_id
    pending = []
    try:
        for pair in PAIRS:
            files = {}
            for spec, symbol in ((pair.left, pair.left_symbol), (pair.right, pair.right_symbol)):
                files[spec.table] = {
                    r["period_start"]: (r["source_file"], r["sha256"])
                    for r in spark.sql(
                        latest_files_query(args.catalog, spec.table, symbol)
                    ).collect()
                }
            compared = {
                (r["day"], r["left_sha256"], r["right_sha256"])
                for r in spark.sql(
                    f"SELECT day, left_sha256, right_sha256 "
                    f"FROM {table_fqn(args.catalog, 'control', CROSS_VENUE_LOG)} "
                    f"WHERE pair = {sql_string(pair.name)}"
                ).collect()
            }
            days = days_to_compare(
                files[pair.left.table],
                files[pair.right.table],
                compared,
                at.date(),
                pair.grace_days,
                args.period,
            )
            print(f"{pair.name}: {len(days)} days to compare")
            if not days:
                if args.fault != NO_FAULT:
                    raise RuntimeError(f"{pair.name}: no days in the period to inject into")
                continue

            day_list = [d.day for d in days]
            left = minute_bars(
                spark.table(table_fqn(args.catalog, "silver", pair.left.table)),
                pair.left_symbol,
                day_list,
            )
            right = minute_candles(
                spark.table(table_fqn(args.catalog, "silver", pair.right.table)),
                pair.right_symbol,
                day_list,
            )
            staging = staging_root / pair.name
            compare(spark, pair, left, right, day_list, args.fault).write.mode("overwrite").parquet(
                str(staging)
            )
            staged = spark.read.parquet(str(staging))
            counts = {
                r["day"]: (r["minutes"], r["divergent"], r["missing_left"], r["missing_right"])
                for r in daily_counts(staged).collect()
            }
            measurements += venue_measurements(
                pair, [(d, c[0], c[1], c[2] + c[3]) for d, c in sorted(counts.items())]
            )
            pending.append((pair, days, staged, counts))

        waivers = {}
        if args.fault == NO_FAULT:
            waivers = {
                (r["gate_check"], r["subject"], r["day"]): r["reason"]
                for r in spark.table(table_fqn(args.catalog, "control", QUALITY_WAIVERS)).collect()
            }
        results = judge(measurements, THRESHOLDS, waivers)
        if results:
            spark.createDataFrame(
                result_rows(results, args.run_id, at, args.fault),
                schema=QUALITY_GATE_RESULTS_SCHEMA,
            ).write.mode("append").saveAsTable(
                table_fqn(args.catalog, "control", QUALITY_GATE_RESULTS)
            )
        for r in results:
            if not r.passed:
                state = f"WAIVED ({r.waiver_reason})" if r.waived else "BREACH"
                print(f"{state}: {r.check} {r.subject} {r.day}: {r.failed} of {r.measured}")
        breaches = [r for r in results if r.breach]
        if breaches:
            raise GateFailure(breaches)
        if args.fault != NO_FAULT:
            raise RuntimeError(f"injected {args.fault} did not stop the gate; nothing was written")

        for pair, days, staged, counts in pending:
            comparison_rows(spark, staged, {d.day: d for d in days}, args.run_id, at).write.mode(
                "overwrite"
            ).option("replaceWhere", day_predicate(pair, [d.day for d in days])).saveAsTable(
                table_fqn(args.catalog, "silver", CROSS_VENUE)
            )
            spark.createDataFrame(
                log_rows(pair, days, counts, args.run_id, at), schema=CROSS_VENUE_LOG_SCHEMA
            ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", CROSS_VENUE_LOG))
            print(f"{pair.name}: wrote the comparison of {len(days)} days")
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    print(f"OK: quality gate passed {len(results)} checks in {args.catalog}")


if __name__ == "__main__":
    main()

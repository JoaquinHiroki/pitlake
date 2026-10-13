import argparse
import shutil
import uuid
from collections import defaultdict
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
from pitlake.gold import (
    BAR_COLUMNS,
    DAILY_BARS,
    FUNDAMENTAL_COLUMNS,
    MINUTE_BAR_SOURCES,
    SILVER_INPUTS,
    MinuteBarSource,
    build_features,
    daily_bars,
    fundamentals,
    gold_fault,
    minute_bars,
)
from pitlake.jobs.commit_transform import latest_silver_commits_query
from pitlake.leakage import (
    LeakageFound,
    audit_instants,
    bar_closed,
    counterfactual,
    feature_after_source,
    filing_visibility,
)
from pitlake.quality import FAULTS, NO_FAULT
from pitlake.silver import (
    EDGAR_COMPANY_FACTS_SILVER,
    EDGAR_FILINGS_SILVER,
    FRED_SERIES_VINTAGES_SILVER,
)
from pitlake.snapshots import readable_version
from pitlake.tables import (
    BARS_1D,
    BARS_1M,
    FEATURES,
    FILING_VISIBILITY,
    FUNDAMENTALS,
    GOLD_BARS_LOG,
    GOLD_BARS_LOG_SCHEMA,
    GOLD_BUILDS,
    GOLD_BUILDS_SCHEMA,
    LEAKAGE_CHECKS,
    LEAKAGE_CHECKS_SCHEMA,
    SILVER_LOAD_LOG,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--period-start", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument(
        "--reprocess", default="false", help="Rebuild the bars of a period; needs a period"
    )
    parser.add_argument("--inject-fault", default=NO_FAULT, choices=FAULTS)
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.period_start = parse_optional_date(args.period_start)
    args.period_end = parse_optional_date(args.period_end)
    args.reprocess = parse_bool(args.reprocess)
    args.fault = gold_fault(args.inject_fault)
    if args.reprocess and (args.period_start is None or args.period_end is None):
        parser.error("--reprocess requires --period-start and --period-end")
    return args


def pending_bar_files_query(
    catalog: str,
    source: MinuteBarSource,
    committed_at: datetime,
    period_start: date | None,
    period_end: date | None,
    reprocess: bool,
) -> tuple[str, dict[str, object]]:
    conditions = ["silver_table = :silver_table", "processed_at <= :committed_at"]
    params: dict[str, object] = {"silver_table": source.spec.table, "committed_at": committed_at}
    if period_start is not None:
        conditions.append("period_start >= :period_start")
        params["period_start"] = period_start
    if period_end is not None:
        conditions.append("period_start <= :period_end")
        params["period_end"] = period_end
    anti_join = ""
    if not reprocess:
        anti_join = (
            f"LEFT ANTI JOIN {table_fqn(catalog, 'control', GOLD_BARS_LOG)} AS g "
            "ON g.venue = :venue AND g.source_file = f.source_file "
            "AND g.silver_processed_at = f.processed_at"
        )
        params["venue"] = source.venue
    query = f"""
        SELECT f.source_file, f.sha256, f.symbol, f.period_start, f.processed_at
        FROM (
          SELECT source_file, sha256, symbol, period_start, processed_at
          FROM {table_fqn(catalog, "control", SILVER_LOAD_LOG)}
          WHERE {" AND ".join(conditions)}
          QUALIFY row_number() OVER (PARTITION BY source_file ORDER BY processed_at DESC) = 1
        ) AS f {anti_join}
        ORDER BY f.period_start, f.source_file
    """
    return query, params


def bar_slices(venue: str, pending: list) -> dict[str, list[date]]:
    periods: dict[str, list[date]] = defaultdict(list)
    for row in pending:
        if row["period_start"] in periods[row["symbol"]]:
            raise RuntimeError(f"{venue} {row['symbol']}: two files for {row['period_start']}")
        periods[row["symbol"]].append(row["period_start"])
    return dict(periods)


def bars_predicate(venue: str, symbol: str, periods: list[date]) -> str:
    if not periods:
        raise ValueError("no periods to replace")
    listed = ", ".join(f"DATE'{p.isoformat()}'" for p in sorted(periods))
    return f"venue = {sql_string(venue)} AND symbol = {sql_string(symbol)} AND period IN ({listed})"


def bars_log_rows(venue, pending, counts, silver_version, run_id, at) -> list[tuple]:
    return [
        (
            venue,
            r["symbol"],
            r["period_start"],
            r["source_file"],
            r["sha256"],
            r["processed_at"],
            silver_version,
            counts.get(r["source_file"], 0),
            at,
            run_id,
        )
        for r in pending
    ]


def check_rows(results, run_id: str, at: datetime, fault: str) -> list[tuple]:
    return [(run_id, at, r.check, r.subject, r.examined, r.cases, r.sample, fault) for r in results]


def silver_snapshots(spark, catalog: str) -> dict[str, tuple[int, datetime]]:
    commits = {
        r["silver_table"]: (r["silver_version"], r["committed_at"])
        for r in spark.sql(latest_silver_commits_query(catalog)).collect()
    }
    missing = sorted(s.table for s in SILVER_INPUTS if s.table not in commits)
    if missing:
        raise RuntimeError(f"no committed Silver version of {missing}; run the transform first")
    snapshots = {}
    for spec in SILVER_INPUTS:
        version, committed_at = commits[spec.table]
        table = table_fqn(catalog, "silver", spec.table)
        snapshots[spec.table] = (readable_version(spark, table, version), committed_at)
    return snapshots


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    at = datetime.now(UTC)

    snapshots = silver_snapshots(spark, args.catalog)
    versions = {table: version for table, (version, _) in snapshots.items()}
    silver = {
        table: spark.sql(
            f"SELECT * FROM {table_fqn(args.catalog, 'silver', table)} VERSION AS OF {int(v)}"
        )
        for table, v in versions.items()
    }
    print(f"reading Silver at {', '.join(f'{t} v{v}' for t, v in sorted(versions.items()))}")

    def stamped(frame):
        return frame.select(
            "*", F.lit(at).cast("timestamp").alias("_built_at"), F.lit(args.run_id).alias("_run_id")
        )

    staging_root = Path(volume_path(args.catalog, "gold", STAGING_VOLUME)) / args.run_id

    def stage(name: str, frame):
        path = str(staging_root / name)
        frame.write.mode("overwrite").parquet(path)
        return spark.read.parquet(path)

    try:
        pending = {}
        new_bars = None
        for source in MINUTE_BAR_SOURCES:
            query, params = pending_bar_files_query(
                args.catalog,
                source,
                snapshots[source.spec.table][1],
                args.period_start,
                args.period_end,
                args.reprocess,
            )
            files = spark.sql(query, args=params).collect()
            print(f"{source.venue}: {len(files)} files to compress into minute bars")
            if not files:
                continue
            pending[source] = files
            chosen = spark.createDataFrame(
                [(r["source_file"],) for r in files], "_source_file STRING"
            )
            rows = (
                silver[source.spec.table]
                .where(F.col("_period_start").isin(sorted({r["period_start"] for r in files})))
                .join(F.broadcast(chosen), "_source_file", "left_semi")
            )
            bars = minute_bars(source, rows).withColumn(
                "_silver_version", F.lit(versions[source.spec.table]).cast("bigint")
            )
            new_bars = bars if new_bars is None else new_bars.unionByName(bars)

        live_1m = spark.table(table_fqn(args.catalog, "gold", BARS_1M)).select(*BAR_COLUMNS)
        if new_bars is None:
            staged_1m = live_1m.limit(0)
            all_1m = live_1m
        else:
            staged_1m = stage(BARS_1M, stamped(new_bars))
            replaced = spark.createDataFrame(
                [
                    (s.venue, r["symbol"], r["period_start"])
                    for s, files in pending.items()
                    for r in files
                ],
                "venue STRING, symbol STRING, period DATE",
            )
            all_1m = live_1m.join(
                F.broadcast(replaced), ["venue", "symbol", "period"], "left_anti"
            ).unionByName(staged_1m.select(*BAR_COLUMNS))

        staged_1d = stage(BARS_1D, stamped(daily_bars(all_1m, silver[DAILY_BARS.table])))
        facts = silver[EDGAR_COMPANY_FACTS_SILVER.table]
        filings = silver[EDGAR_FILINGS_SILVER.table]
        vintages = silver[FRED_SERIES_VINTAGES_SILVER.table]
        staged_fundamentals = stage(
            FUNDAMENTALS,
            stamped(
                fundamentals(spark, facts).select(
                    *FUNDAMENTAL_COLUMNS,
                    F.lit(versions[EDGAR_COMPANY_FACTS_SILVER.table])
                    .cast("bigint")
                    .alias("_silver_version"),
                )
            ),
        )
        staged_features = stage(
            FEATURES, stamped(build_features(spark, staged_1d, facts, vintages, args.fault))
        )

        def rebuild(instant: datetime):
            def known(frame):
                return frame.where(F.col("knowable_at") <= F.lit(instant).cast("timestamp"))

            return build_features(
                spark, known(staged_1d), known(facts), known(vintages), args.fault
            )

        results = bar_closed(staged_1m, staged_1d)
        results += feature_after_source(staged_features, facts, filings, staged_1d)
        visibility, filed = filing_visibility(staged_features, facts, filings)
        visibility = stage(FILING_VISIBILITY, visibility)
        results.append(filed)
        instants = audit_instants(visibility, vintages, staged_1d)
        print(f"counterfactual: rebuilding the features as known at {len(instants)} instants")
        results += counterfactual(staged_features, rebuild, instants)

        spark.createDataFrame(
            check_rows(results, args.run_id, at, args.fault), schema=LEAKAGE_CHECKS_SCHEMA
        ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", LEAKAGE_CHECKS))
        visibility.select(
            F.lit(args.run_id).alias("run_id"),
            F.lit(at).cast("timestamp").alias("checked_at"),
            "*",
            F.lit(args.fault).alias("injected_fault"),
        ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", FILING_VISIBILITY))
        for r in results:
            state = "LEAK" if r.cases else "ok"
            print(f"{state}: {r.check} {r.subject}: {r.cases} cases in {r.examined} rows")
        if any(r.cases for r in results):
            raise LeakageFound(results)
        if args.fault != NO_FAULT:
            raise RuntimeError(f"injected {args.fault} was not detected; nothing was written")

        written = {}
        for source, files in pending.items():
            counts = {
                r["_source_file"]: r["count"]
                for r in staged_1m.where(F.col("venue") == source.venue)
                .groupBy("_source_file")
                .count()
                .collect()
            }
            for symbol, periods in bar_slices(source.venue, files).items():
                staged_1m.where(
                    (F.col("venue") == source.venue) & (F.col("symbol") == symbol)
                ).write.mode("overwrite").option(
                    "replaceWhere", bars_predicate(source.venue, symbol, periods)
                ).saveAsTable(table_fqn(args.catalog, "gold", BARS_1M))
            spark.createDataFrame(
                bars_log_rows(
                    source.venue,
                    files,
                    counts,
                    versions[source.spec.table],
                    args.run_id,
                    at,
                ),
                schema=GOLD_BARS_LOG_SCHEMA,
            ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", GOLD_BARS_LOG))
            written[BARS_1M] = written.get(BARS_1M, 0) + sum(counts.values())
        for table, staged in (
            (BARS_1D, staged_1d),
            (FUNDAMENTALS, staged_fundamentals),
            (FEATURES, staged_features),
        ):
            staged.writeTo(table_fqn(args.catalog, "gold", table)).overwrite(F.lit(True))
            written[table] = staged.count()
        written.setdefault(BARS_1M, 0)
        spark.createDataFrame(
            [(args.run_id, at, t, versions, n) for t, n in sorted(written.items())],
            schema=GOLD_BUILDS_SCHEMA,
        ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", GOLD_BUILDS))
        for table, n in sorted(written.items()):
            print(f"wrote {n} rows to gold.{table}")
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
    print(f"OK: Gold built and audited in {args.catalog}: no leakage in {len(results)} checks")


if __name__ == "__main__":
    main()

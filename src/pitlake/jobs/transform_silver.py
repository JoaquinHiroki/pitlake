import argparse
import json
import shutil
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

from pitlake.bronze import (
    batched,
    parse_bool,
    parse_optional_date,
    replace_where_predicate,
    sql_string,
)
from pitlake.cleaning import (
    FAILED_RULE,
    RECORD,
    evaluate,
    prepare,
    quarantine_rows,
    sequence_gaps,
    silver_rows,
)
from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.jobs.commit_ingest import latest_commits_query
from pitlake.quality import FAULTS, KNOWABLE_FAULTS, NO_FAULT, QualityHalt, halt_violations
from pitlake.silver import SILVER_SPECS, SilverSpec, parse_keys
from pitlake.snapshots import readable_version
from pitlake.tables import (
    QUALITY_HALTS,
    QUALITY_HALTS_SCHEMA,
    QUARANTINE,
    SEQUENCE_GAPS,
    SILVER_LOAD_LOG,
    SILVER_LOAD_LOG_SCHEMA,
    SOURCE_REGISTRY,
)

HALT_SAMPLE_ROWS = 5


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--datasets",
        action="append",
        help="Comma-separated dataset keys such as edgar.filings,edgar.company_facts. Default: all",
    )
    parser.add_argument(
        "--max-files", type=int, default=30, help="Files per table in this run; 0 means no limit"
    )
    parser.add_argument(
        "--files-per-batch", type=int, help="Override the dataset's own files_per_commit"
    )
    parser.add_argument("--period-start", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument(
        "--reprocess", default="false", help="Transform files Silver already has; needs a period"
    )
    parser.add_argument("--inject-fault", default=NO_FAULT, choices=FAULTS)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    chosen = set().union(*(parse_keys(v) for v in args.datasets or ())) or set(SILVER_SPECS)
    args.specs = [s for k, s in SILVER_SPECS.items() if k in chosen]
    args.period_start = parse_optional_date(args.period_start)
    args.period_end = parse_optional_date(args.period_end)
    args.reprocess = parse_bool(args.reprocess)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    if args.max_files < 0:
        parser.error("--max-files must be 0 or positive")
    if args.files_per_batch is not None and args.files_per_batch < 1:
        parser.error("--files-per-batch must be at least 1")
    if args.reprocess and (args.period_start is None or args.period_end is None):
        parser.error("--reprocess requires --period-start and --period-end")
    if args.inject_fault not in KNOWABLE_FAULTS:
        args.inject_fault = NO_FAULT
    if args.inject_fault != NO_FAULT and not args.reprocess:
        parser.error("--inject-fault requires --reprocess, so the fault has rows to hit")
    return args


def pending_files_query(
    catalog: str,
    spec: SilverSpec,
    bronze_version: int,
    period_start: date | None,
    period_end: date | None,
    reprocess: bool,
    limit: int,
) -> tuple[str, dict[str, object]]:
    conditions = ["true"]
    params: dict[str, object] = {}
    if period_start is not None:
        conditions.append("_period_start >= :period_start")
        params["period_start"] = period_start
    if period_end is not None:
        conditions.append("_period_start <= :period_end")
        params["period_end"] = period_end
    anti_join = ""
    if not reprocess:
        anti_join = (
            f"LEFT ANTI JOIN {table_fqn(catalog, 'control', SILVER_LOAD_LOG)} AS l "
            "ON l.silver_table = :silver_table "
            "AND b._source_file = l.source_file AND b._source_sha256 = l.sha256"
        )
        params["silver_table"] = spec.table
    query = f"""
        SELECT b._source_file, b._source_sha256, b._symbol, b._period_start
        FROM (
          SELECT DISTINCT _source_file, _source_sha256, _symbol, _period_start
          FROM {table_fqn(catalog, "bronze", spec.dataset.bronze_table)}
            VERSION AS OF {int(bronze_version)}
          WHERE {" AND ".join(conditions)}
        ) AS b {anti_join}
        ORDER BY b._period_start, b._source_file
    """
    if limit and spec.scope == "file":
        query += f" LIMIT {int(limit)}"
    return query, params


def symbol_predicate(symbols: list[str]) -> str:
    if not symbols:
        raise ValueError("no symbols to replace")
    return f"_symbol IN ({', '.join(sql_string(s) for s in symbols)})"


def shared_predicate(spec: SilverSpec, predicate: str) -> str:
    return f"silver_table = {sql_string(spec.table)} AND ({predicate})"


def log_rows(
    spec: SilverSpec,
    counts: list,
    gaps: dict[str, tuple[int, int]],
    bronze_version: int,
    run_id: str,
    processed_at: datetime,
) -> list[tuple]:
    files: dict[str, dict] = {}
    for file, sha256, symbol, period_start, rule, rows in counts:
        entry = files.setdefault(
            file,
            {"sha256": sha256, "symbol": symbol, "period_start": period_start, "rules": {}},
        )
        entry["rules"][rule] = entry["rules"].get(rule, 0) + rows
    result = []
    for file in sorted(files):
        entry = files[file]
        rows_in = sum(entry["rules"].values())
        rows_out = entry["rules"].get(None, 0)
        rejected = {rule: entry["rules"][rule] for rule in sorted(r for r in entry["rules"] if r)}
        result.append(
            (
                spec.table,
                file,
                entry["sha256"],
                entry["symbol"],
                entry["period_start"],
                bronze_version,
                rows_in,
                rows_out,
                rows_in - rows_out,
                rejected,
                *gaps.get(file, (0, 0)),
                processed_at,
                run_id,
            )
        )
    return result


def record_halt(spark, catalog, spec, staged, violations, run_id, fault) -> None:
    from pyspark.sql import functions as F

    halted_at = datetime.now(UTC)
    rows = []
    for rule, failed in sorted(violations.items()):
        sample = [
            json.loads(r["row"])
            for r in staged.where(F.col(FAILED_RULE) == rule)
            .select(
                F.to_json(F.struct("event_time", "knowable_at", "_source_file", RECORD)).alias(
                    "row"
                )
            )
            .limit(HALT_SAMPLE_ROWS)
            .collect()
        ]
        rows.append((run_id, halted_at, spec.table, rule, failed, json.dumps(sample), fault))
    spark.createDataFrame(rows, schema=QUALITY_HALTS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(catalog, "control", QUALITY_HALTS)
    )


def transform_batch(spark, args, spec, bronze, bronze_version, registry, lookups, batch) -> dict:
    from pyspark.sql import functions as F

    if spec.scope == "file":
        files = [row._source_file for row in batch]
        days = sorted({row._period_start for row in batch})
        rows = bronze.where(F.col("_source_file").isin(files) & F.col("_period_start").isin(days))
        predicate = replace_where_predicate(files)
    else:
        symbols = sorted({row._symbol for row in batch})
        rows = bronze.where(F.col("_symbol").isin(symbols))
        predicate = symbol_predicate(symbols)

    evaluated = evaluate(spec, prepare(spec, rows, registry, lookups, args.inject_fault))
    staging_root = Path(volume_path(args.catalog, "silver", STAGING_VOLUME))
    staging = staging_root / args.run_id / uuid.uuid4().hex
    try:
        evaluated.write.mode("overwrite").parquet(str(staging))
        staged = spark.read.parquet(str(staging))
        group = ["_source_file", "_source_sha256", "_symbol", "_period_start", FAILED_RULE]
        counts = [tuple(r) for r in staged.groupBy(*group).count().collect()]
        by_rule: dict[str | None, int] = {}
        for *_, rule, n in counts:
            by_rule[rule] = by_rule.get(rule, 0) + n

        violations = halt_violations(spec.rules, by_rule)
        if violations:
            record_halt(
                spark, args.catalog, spec, staged, violations, args.run_id, args.inject_fault
            )
            raise QualityHalt(spec.table, violations)
        if args.inject_fault != NO_FAULT:
            raise RuntimeError(
                f"{spec.table}: injected {args.inject_fault} did not halt the run; nothing was "
                "written, but the halt rules are not working"
            )

        at = datetime.now(UTC)
        silver_rows(spec, staged, bronze_version, args.run_id, at).write.mode("overwrite").option(
            "replaceWhere", predicate
        ).saveAsTable(table_fqn(args.catalog, "silver", spec.table))
        quarantine_rows(spec, staged, bronze_version, args.run_id, at).write.mode(
            "overwrite"
        ).option("replaceWhere", shared_predicate(spec, predicate)).saveAsTable(
            table_fqn(args.catalog, "silver", QUARANTINE)
        )

        gaps: dict[str, tuple[int, int]] = {}
        gap_rows = sequence_gaps(spec, staged, args.run_id, at)
        if gap_rows is not None:
            gaps_table = table_fqn(args.catalog, "silver", SEQUENCE_GAPS)
            gap_rows.write.mode("overwrite").option(
                "replaceWhere", shared_predicate(spec, predicate)
            ).saveAsTable(gaps_table)
            gaps = {
                r["_source_file"]: (r["gaps"], r["missing"])
                for r in spark.table(gaps_table)
                .where(F.expr(shared_predicate(spec, predicate)))
                .groupBy("_source_file")
                .agg(F.count("*").alias("gaps"), F.sum("missing").alias("missing"))
                .collect()
            }

        log = log_rows(spec, counts, gaps, bronze_version, args.run_id, at)
        spark.createDataFrame(log, schema=SILVER_LOAD_LOG_SCHEMA).write.mode("append").saveAsTable(
            table_fqn(args.catalog, "control", SILVER_LOAD_LOG)
        )
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    rejected = {rule: n for rule, n in by_rule.items() if rule is not None}
    return {
        "files": len(log),
        "rows_in": sum(by_rule.values()),
        "rows_out": by_rule.get(None, 0),
        "rejected": rejected,
        "gaps": sum(g for g, _ in gaps.values()),
    }


def transform_table(spark, args, spec: SilverSpec, commits: dict, registry) -> None:
    bronze_version = commits.get((spec.dataset.source, spec.dataset.name))
    if bronze_version is None:
        if args.inject_fault != NO_FAULT:
            raise RuntimeError(f"{spec.key_name}: no committed Bronze version to inject into")
        print(f"{spec.key_name}: no committed Bronze version yet, nothing to transform")
        return
    bronze_table = table_fqn(args.catalog, "bronze", spec.dataset.bronze_table)
    bronze_version = readable_version(spark, bronze_table, bronze_version)

    query, params = pending_files_query(
        args.catalog,
        spec,
        bronze_version,
        args.period_start,
        args.period_end,
        args.reprocess,
        args.max_files,
    )
    pending = spark.sql(query, args=params).collect()
    print(f"{spec.key_name}: {len(pending)} files to transform, Bronze version {bronze_version}")
    if not pending:
        if args.inject_fault != NO_FAULT:
            raise RuntimeError(f"{spec.key_name}: no files in the period to inject into")
        return

    bronze = spark.sql(f"SELECT * FROM {bronze_table} VERSION AS OF {int(bronze_version)}")
    lookups = {
        lookup.table: spark.table(table_fqn(args.catalog, "silver", lookup.table))
        for lookup in spec.lookups
    }
    size = args.files_per_batch or spec.dataset.files_per_commit
    batches = batched(pending, size) if spec.scope == "file" else [pending]

    totals = {"files": 0, "rows_in": 0, "rows_out": 0, "gaps": 0}
    rejected: dict[str, int] = {}
    for batch in batches:
        result = transform_batch(
            spark, args, spec, bronze, bronze_version, registry, lookups, batch
        )
        for key in totals:
            totals[key] += result[key]
        for rule, n in result["rejected"].items():
            rejected[rule] = rejected.get(rule, 0) + n
        print(
            f"{spec.key_name}: committed {totals['files']} files, {totals['rows_in']} rows judged"
        )
    print(
        f"OK: {spec.key_name} transformed {totals['files']} files: {totals['rows_in']} rows in, "
        f"{totals['rows_out']} to Silver, rejected {json.dumps(rejected, sort_keys=True)}, "
        f"{totals['gaps']} sequence gaps"
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    commits = {
        (r["source"], r["dataset"]): r["bronze_version"]
        for r in spark.sql(latest_commits_query(args.catalog)).collect()
    }
    registry = spark.table(table_fqn(args.catalog, "control", SOURCE_REGISTRY))
    for spec in args.specs:
        transform_table(spark, args, spec, commits, registry)
    print(f"OK: transformed {', '.join(s.key_name for s in args.specs)} in {args.catalog}")


if __name__ == "__main__":
    main()

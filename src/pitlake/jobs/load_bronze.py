"""Ingest task 2: load landed files that are not yet in Bronze.

Idempotency comes from the write, not from bookkeeping: each commit overwrites exactly the
rows whose _source_file is in the batch (Delta replaceWhere), so loading a file twice leaves
one copy of its rows. The load log only decides what is pending; if a run dies between the
write and the log append, the next run reloads the same files and the result is unchanged.
"""

import argparse
import shutil
import uuid
from datetime import UTC, date, datetime
from functools import reduce
from pathlib import Path

from pitlake.bronze import (
    batched,
    extract_single_member,
    parse_bool,
    parse_optional_date,
    replace_where_predicate,
    verify_sha256,
)
from pitlake.config import (
    STAGING_VOLUME,
    landing_file_path,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.datasets import DATASETS, Dataset, get_dataset
from pitlake.readers import read_raw
from pitlake.tables import (
    BRONZE_LOAD_LOG,
    BRONZE_LOAD_LOG_SCHEMA,
    BRONZE_METADATA_COLUMNS,
    CORRUPT_RECORD_COLUMN,
    LANDING_MANIFEST,
    bronze_ddl,
    bronze_load_log_ddl,
    landing_manifest_ddl,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--dataset", action="append", help="Dataset key such as binance.spot_trades. Default: all"
    )
    parser.add_argument(
        "--max-files", type=int, default=30, help="Files to load in this run; 0 means no limit"
    )
    parser.add_argument(
        "--files-per-commit", type=int, help="Override the dataset's own files_per_commit"
    )
    parser.add_argument("--period-start", default="")
    parser.add_argument("--period-end", default="")
    parser.add_argument(
        "--reload", default="false", help="Reload files already in Bronze; needs a period"
    )
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.datasets = (
        [get_dataset(k) for k in args.dataset] if args.dataset else list(DATASETS.values())
    )
    args.period_start = parse_optional_date(args.period_start)
    args.period_end = parse_optional_date(args.period_end)
    args.reload = parse_bool(args.reload)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    if args.max_files < 0:
        parser.error("--max-files must be 0 or positive")
    if args.reload and (args.period_start is None or args.period_end is None):
        parser.error("--reload requires --period-start and --period-end")
    return args


def select_pending(
    spark,
    catalog: str,
    dataset: Dataset,
    period_start: date | None,
    period_end: date | None,
    reload: bool,
    limit: int,
) -> list:
    conditions = ["source = :source", "dataset = :dataset"]
    params: dict[str, object] = {"source": dataset.source, "dataset": dataset.name}
    if period_start is not None:
        conditions.append("period_start >= :period_start")
        params["period_start"] = period_start
    if period_end is not None:
        conditions.append("period_end <= :period_end")
        params["period_end"] = period_end

    anti_join = (
        ""
        if reload
        else f"LEFT ANTI JOIN {table_fqn(catalog, 'control', BRONZE_LOAD_LOG)} AS l "
        "ON m.landing_path = l.landing_path AND m.sha256 = l.sha256"
    )
    query = f"""
        WITH latest AS (
          SELECT * FROM {table_fqn(catalog, "control", LANDING_MANIFEST)}
          WHERE {" AND ".join(conditions)}
          QUALIFY row_number() OVER (
            PARTITION BY landing_path ORDER BY fetched_at DESC, _discovered_at DESC) = 1
        )
        SELECT m.landing_path, m.sha256, m.symbol, m.period_start
        FROM latest AS m {anti_join}
        ORDER BY m.period_start, m.landing_path
    """
    if limit:
        query += f" LIMIT {int(limit)}"
    return spark.sql(query, args=params).collect()


def load_batch(spark, catalog: str, dataset: Dataset, rows: list, run_id: str) -> int:
    from pyspark.sql import functions as F

    bronze = table_fqn(catalog, "bronze", dataset.bronze_table)
    staging = Path(volume_path(catalog, "bronze", STAGING_VOLUME)) / run_id
    source_files = [row.landing_path for row in rows]
    try:
        frames = []
        for row in rows:
            landed = landing_file_path(catalog, row.landing_path)
            verify_sha256(landed, row.sha256)
            data_file = (
                extract_single_member(landed, staging / row.sha256)
                if dataset.archive == "zip"
                else landed
            )
            frames.append(
                read_raw(spark, dataset, str(data_file)).withColumns(
                    {
                        "_source": F.lit(dataset.source),
                        "_dataset": F.lit(dataset.name),
                        "_symbol": F.lit(row.symbol),
                        "_period_start": F.lit(row.period_start),
                        "_source_file": F.lit(row.landing_path),
                        "_source_sha256": F.lit(row.sha256),
                        "_ingested_at": F.current_timestamp(),
                        "_load_run_id": F.lit(run_id),
                    }
                )
            )
        columns = [
            *dataset.columns,
            CORRUPT_RECORD_COLUMN,
            *(n for n, _ in BRONZE_METADATA_COLUMNS),
        ]
        (
            # Call the method on the instance: on serverless these are Spark Connect DataFrames,
            # and the classic pyspark.sql.DataFrame.unionByName reaches for the JVM (_jdf).
            reduce(lambda left, right: left.unionByName(right), frames)
            .select(*columns)
            .write.mode("overwrite")
            .option("replaceWhere", replace_where_predicate(source_files))
            .saveAsTable(bronze)
        )
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    counts = {
        r["_source_file"]: (r["row_count"], r["corrupt_row_count"])
        for r in spark.table(bronze)
        .where(F.col("_source_file").isin(source_files))
        .groupBy("_source_file")
        .agg(
            F.count("*").alias("row_count"),
            F.count(CORRUPT_RECORD_COLUMN).alias("corrupt_row_count"),
        )
        .collect()
    }
    loaded_at = datetime.now(UTC)
    log_rows = [
        (
            row.landing_path,
            row.sha256,
            dataset.source,
            dataset.name,
            row.symbol,
            row.period_start,
            *counts.get(row.landing_path, (0, 0)),
            loaded_at,
            run_id,
        )
        for row in rows
    ]
    spark.createDataFrame(log_rows, schema=BRONZE_LOAD_LOG_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(catalog, "control", BRONZE_LOAD_LOG)
    )
    return sum(c[0] for c in counts.values())


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(landing_manifest_ddl(args.catalog))
    spark.sql(bronze_load_log_ddl(args.catalog))

    for dataset in args.datasets:
        spark.sql(bronze_ddl(args.catalog, dataset))
        pending = select_pending(
            spark,
            args.catalog,
            dataset,
            args.period_start,
            args.period_end,
            args.reload,
            args.max_files,
        )
        print(f"{dataset.key}: {len(pending)} files to load")
        loaded_files = loaded_rows = 0
        for batch in batched(pending, args.files_per_commit or dataset.files_per_commit):
            loaded_rows += load_batch(spark, args.catalog, dataset, batch, args.run_id)
            loaded_files += len(batch)
            print(f"{dataset.key}: committed {loaded_files}/{len(pending)} files")
        print(f"OK: {dataset.key} loaded {loaded_files} files, {loaded_rows} rows")


if __name__ == "__main__":
    main()

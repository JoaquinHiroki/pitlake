import argparse
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.jobs.commit_transform import latest_silver_commits_query
from pitlake.rag import build_row, filing_chunks, latest_version
from pitlake.silver import EDGAR_FILING_DOCUMENTS_SILVER
from pitlake.snapshots import readable_version
from pitlake.tables import FILING_CHUNKS, RAG_BUILDS, RAG_BUILDS_SCHEMA, rag_ddls

SILVER_KEY = f"silver.{EDGAR_FILING_DOCUMENTS_SILVER.table}"
CHUNK_COLUMNS = (
    "chunk_id",
    "symbol",
    "accession_number",
    "form",
    "filing_date",
    "report_date",
    "section",
    "chunk_index",
    "context",
    "text",
    "knowable_at",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in rag_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)

    silver = EDGAR_FILING_DOCUMENTS_SILVER.table
    commits = {
        r["silver_table"]: r["silver_version"]
        for r in spark.sql(latest_silver_commits_query(args.catalog)).collect()
    }
    if silver not in commits:
        raise RuntimeError(
            f"no committed {SILVER_KEY} yet; run the ingest and transform jobs first"
        )
    source = table_fqn(args.catalog, "silver", silver)
    version = readable_version(spark, source, commits[silver])
    documents = spark.sql(f"SELECT * FROM {source} VERSION AS OF {int(version)}")
    count = documents.count()
    print(f"chunking {count} filings from {SILVER_KEY} version {version}")

    target = table_fqn(args.catalog, "gold", FILING_CHUNKS)
    staging = Path(volume_path(args.catalog, "gold", STAGING_VOLUME)) / args.run_id
    try:
        filing_chunks(documents, count).select(
            *CHUNK_COLUMNS,
            F.lit(version).cast("bigint").alias("_silver_version"),
            F.lit(at).cast("timestamp").alias("_built_at"),
            F.lit(args.run_id).alias("_run_id"),
        ).write.mode("overwrite").parquet(str(staging / FILING_CHUNKS))
        staged = spark.read.parquet(str(staging / FILING_CHUNKS))

        total = staged.count()
        if staged.select("chunk_id").distinct().count() != total:
            raise RuntimeError("two chunks share a chunk_id")
        current = spark.table(target).select("chunk_id")
        added = staged.select("chunk_id").subtract(current).count()
        removed = current.subtract(staged.select("chunk_id")).count()
        if added or removed:
            staged.writeTo(target).overwrite(F.lit(True))
            print(f"gold.{FILING_CHUNKS}: {added} chunks added, {removed} removed, {total} in all")
        else:
            print(f"gold.{FILING_CHUNKS}: unchanged, {total} chunks")
        row = build_row(
            args.run_id,
            at,
            "chunk",
            FILING_CHUNKS,
            latest_version(spark, target),
            total,
            added + removed,
            {SILVER_KEY: version},
            f"{count} filings",
        )
        spark.createDataFrame([row], schema=RAG_BUILDS_SCHEMA).write.mode("append").saveAsTable(
            table_fqn(args.catalog, "control", RAG_BUILDS)
        )
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    print(f"OK: {total} chunks of {count} filings in {args.catalog}")


if __name__ == "__main__":
    main()

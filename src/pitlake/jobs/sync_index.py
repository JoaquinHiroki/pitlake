import argparse
import uuid
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.rag import (
    build_row,
    ensure_endpoint,
    ensure_index,
    index_source,
    latest_version,
    vector_index,
    wait_for_index,
)
from pitlake.tables import CHUNK_EMBEDDINGS, FILING_CHUNKS, RAG_BUILDS, RAG_BUILDS_SCHEMA


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint", required=True, help="Vector search endpoint")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.endpoint = validate_segment(args.endpoint.strip())
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    at = datetime.now(UTC)
    source = table_fqn(args.catalog, "gold", CHUNK_EMBEDDINGS)
    chunks = spark.table(table_fqn(args.catalog, "gold", FILING_CHUNKS)).count()
    stats = spark.sql(
        "SELECT count(*) AS n, min(size(embedding)) AS lo, max(size(embedding)) AS hi "
        f"FROM {source}"
    ).collect()[0]
    if not stats["n"]:
        raise RuntimeError(f"gold.{CHUNK_EMBEDDINGS} is empty; run embed first")
    if stats["lo"] != stats["hi"]:
        raise RuntimeError(f"embeddings have {stats['lo']} to {stats['hi']} dimensions")
    if stats["n"] != chunks:
        print(f"note: {stats['n']} of {chunks} chunks are embedded; the index holds only those")

    client = WorkspaceClient()
    index = vector_index(args.catalog)
    ensure_endpoint(client, args.endpoint)
    if not ensure_index(client, args.endpoint, index, index_source(args.catalog), stats["hi"]):
        client.vector_search_indexes.sync_index(index)
        print(f"syncing {index}")
    version = latest_version(spark, source)
    rows = wait_for_index(client, index, stats["n"])
    row = build_row(
        args.run_id,
        at,
        "index",
        CHUNK_EMBEDDINGS,
        version,
        rows,
        0,
        {f"gold.{CHUNK_EMBEDDINGS}": version},
        f"{index} on {args.endpoint}, {stats['hi']} dimensions",
    )
    spark.createDataFrame([row], schema=RAG_BUILDS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", RAG_BUILDS)
    )
    print(f"OK: {index} serves {rows} chunks of gold.{CHUNK_EMBEDDINGS} version {version}")


if __name__ == "__main__":
    main()

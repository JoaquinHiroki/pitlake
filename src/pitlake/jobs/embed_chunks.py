import argparse
import uuid
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.rag import Embedder, build_row, embedding_text, latest_version
from pitlake.retrieval import as_matrix, normalize_rows
from pitlake.tables import (
    CHUNK_EMBEDDINGS,
    CHUNK_EMBEDDINGS_SCHEMA,
    FILING_CHUNKS,
    RAG_BUILDS,
    RAG_BUILDS_SCHEMA,
    rag_ddls,
)

WRITE_BATCH = 1000
COLUMNS = (
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
    parser.add_argument("--endpoint", required=True, help="Embedding model serving endpoint")
    parser.add_argument("--max-chunks", default="0", help="Embed at most this many; 0 = all")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.endpoint = validate_segment(args.endpoint.strip())
    args.max_chunks = int(args.max_chunks or 0)
    if args.max_chunks < 0:
        parser.error("--max-chunks must be 0 or more")
    return args


def embedded_rows(rows, vectors, model: str, run_id: str, at: datetime) -> list[tuple]:
    unit = normalize_rows(as_matrix(vectors)) if vectors else []
    return [
        (
            *(r[c] for c in COLUMNS),
            r["knowable_at_us"],
            [float(x) for x in v],
            model,
            at,
            run_id,
        )
        for r, v in zip(rows, unit, strict=True)
    ]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in rag_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)
    chunks_table = table_fqn(args.catalog, "gold", FILING_CHUNKS)
    target = table_fqn(args.catalog, "gold", CHUNK_EMBEDDINGS)

    models = [r[0] for r in spark.sql(f"SELECT DISTINCT embedding_model FROM {target}").collect()]
    if set(models) - {args.endpoint}:
        raise RuntimeError(
            f"gold.{CHUNK_EMBEDDINGS} holds embeddings of {models}, not {args.endpoint}: drop the "
            "vector index and the table before switching models (runbook stage 7)"
        )
    removed = spark.sql(
        f"DELETE FROM {target} WHERE chunk_id NOT IN (SELECT chunk_id FROM {chunks_table})"
    ).collect()
    removed = int(removed[0]["num_affected_rows"]) if removed else 0

    todo = (
        spark.table(chunks_table)
        .join(spark.table(target).select("chunk_id"), "chunk_id", "left_anti")
        .select(*COLUMNS, F.unix_micros("knowable_at").alias("knowable_at_us"))
        .orderBy("knowable_at_us", "chunk_id")
    )
    pending = todo.count()
    if args.max_chunks:
        todo = todo.limit(args.max_chunks)
    rows = todo.collect()
    print(f"{pending} chunks without an embedding; embedding {len(rows)} with {args.endpoint}")

    embed = Embedder(WorkspaceClient(), args.endpoint)
    for start in range(0, len(rows), WRITE_BATCH):
        batch = rows[start : start + WRITE_BATCH]
        vectors = embed([embedding_text(r["context"], r["text"]) for r in batch])
        spark.createDataFrame(
            embedded_rows(batch, vectors, args.endpoint, args.run_id, at),
            schema=CHUNK_EMBEDDINGS_SCHEMA,
        ).write.mode("append").saveAsTable(target)
        print(f"embedded {start + len(batch)} of {len(rows)} ({embed.dimension} dimensions)")

    total = spark.table(target).count()
    row = build_row(
        args.run_id,
        at,
        "embed",
        CHUNK_EMBEDDINGS,
        latest_version(spark, target),
        total,
        len(rows) + removed,
        {f"gold.{FILING_CHUNKS}": latest_version(spark, chunks_table)},
        f"{args.endpoint}; {pending - len(rows)} chunks left for the next run",
    )
    spark.createDataFrame([row], schema=RAG_BUILDS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", RAG_BUILDS)
    )
    left = pending - len(rows)
    print(f"OK: {total} chunks embedded in {args.catalog}, {removed} removed, {left} left")


if __name__ == "__main__":
    main()

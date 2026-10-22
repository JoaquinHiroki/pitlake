import argparse
import json
import shutil
import uuid
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.generator import MANIFEST, Asked, example, generator_folder, write_jsonl
from pitlake.jobs.ask import tuned_run_query
from pitlake.jobs.tune_retriever import (
    EMBEDDINGS_KEY,
    QUESTIONS_KEY,
    TUNED,
    load_adapter,
    load_corpus,
)
from pitlake.questions import SPLITS
from pitlake.rag import EXCERPTS, Excerpt, instant
from pitlake.retrieval import Query, as_matrix, normalize_rows, rank
from pitlake.tables import RAG_QUESTIONS, RETRIEVAL_RUNS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--retrieval-run", default="", help="Default: the latest tuned run")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.retrieval_run = args.retrieval_run.strip()
    if args.retrieval_run:
        validate_segment(args.retrieval_run)
    return args


def read_asked(spark, catalog: str, version: int) -> tuple[list[Asked], dict]:
    frame = spark.sql(
        "SELECT question_id, split, symbol, metric, duration, period_end, "
        "CAST(value AS STRING) AS value, as_of_us, text, answers, relevant, embedding "
        f"FROM {table_fqn(catalog, 'gold', RAG_QUESTIONS)} VERSION AS OF {int(version)}"
    ).toPandas()
    if frame.empty:
        raise RuntimeError(f"{QUESTIONS_KEY} version {version} is empty")
    rows = sorted(frame.to_dict("records"), key=lambda r: r["question_id"])
    vectors = normalize_rows(as_matrix([r["embedding"] for r in rows]))
    asked = [
        Asked(
            r["question_id"],
            r["split"],
            r["symbol"],
            r["metric"],
            r["duration"],
            r["period_end"],
            Decimal(r["value"]),
            instant(r["as_of_us"]),
            r["text"],
            tuple(r["answers"]),
            frozenset(r["relevant"]),
        )
        for r in rows
    ]
    queries = {
        a.question_id: Query(a.question_id, v, int(r["as_of_us"]), a.symbol, a.relevant)
        for a, r, v in zip(asked, rows, vectors, strict=True)
    }
    return asked, queries


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import mlflow
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    mlflow.set_tracking_uri("databricks")

    query, params = tuned_run_query(args.catalog, args.retrieval_run)
    found = spark.sql(query, args=params).collect()
    if not found:
        raise RuntimeError("no tuned retrieval run recorded yet; run the rag job first")
    run = found[0]
    versions = dict(
        spark.sql(
            f"SELECT table_versions FROM {table_fqn(args.catalog, 'control', RETRIEVAL_RUNS)} "
            "WHERE retrieval_run_id = :id AND retriever = :tuned AND replay_of IS NULL LIMIT 1",
            args={"id": run["retrieval_run_id"], "tuned": TUNED},
        ).collect()[0]["table_versions"]
    )
    adapter = load_adapter(mlflow, run["mlflow_run_id"], run["adapter_sha256"])
    corpus, chunks = load_corpus(spark, args.catalog, versions[EMBEDDINGS_KEY])
    asked, queries = read_asked(spark, args.catalog, versions[QUESTIONS_KEY])

    hits = rank(corpus, [queries[a.question_id] for a in asked], adapter, k=EXCERPTS)
    future = sum(r.future for r in hits)
    if future:
        raise RuntimeError(f"{future} retrieved passages were not knowable when asked")
    examples = {s: [] for s in SPLITS}
    for item, ranking in zip(asked, hits, strict=True):
        ids = [corpus.ids[h.index] for h in ranking.hits]
        excerpts = [Excerpt(c, chunks[c].context, chunks[c].text) for c in ids]
        examples[item.split].append(example(item, excerpts))

    folder = generator_folder(volume_path(args.catalog, "gold", STAGING_VOLUME), args.run_id)
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    counts = {s: write_jsonl(folder / f"{s}.jsonl", rows) for s, rows in examples.items()}
    answerable = {
        s: dict(Counter("answer" if r["cited"] else "abstain" for r in rows))
        for s, rows in examples.items()
    }
    manifest = {
        "run_id": args.run_id,
        "catalog": args.catalog,
        "prepared_at": datetime.now(UTC).isoformat(),
        "retrieval_run_id": run["retrieval_run_id"],
        "adapter_sha256": run["adapter_sha256"],
        "embedding_model": run["embedding_model"],
        "table_versions": versions,
        "excerpts": EXCERPTS,
        "examples": counts,
        "targets": answerable,
    }
    (folder / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, indent=2, sort_keys=True))
    if not (counts["train"] and counts["validation"] and counts["test"]):
        raise RuntimeError(f"every split needs examples, got {counts}")
    print(f"OK: wrote {sum(counts.values())} generator examples to {folder}")


if __name__ == "__main__":
    main()

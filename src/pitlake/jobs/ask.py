import argparse
import json
import re
import uuid
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.jobs.tune_retriever import TUNED, load_adapter
from pitlake.rag import (
    EXCERPTS,
    Embedder,
    Excerpt,
    chat,
    citations,
    micros,
    prompt_messages,
    query_index,
    vector_index,
)
from pitlake.retrieval import as_matrix, normalize_rows
from pitlake.tables import RAG_ANSWERS, RAG_ANSWERS_SCHEMA, RETRIEVAL_RUNS, rag_ddls

_SYMBOL = re.compile(r"^[A-Z]{1,5}([.-][A-Z]{1,2})?$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--embedding-endpoint", required=True)
    parser.add_argument("--chat-endpoint", required=True)
    parser.add_argument("--question", required=True)
    parser.add_argument("--as-of", default="", help="ISO instant; default now")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--retrieval-run", default="", help="Default: the latest tuned run")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.embedding_endpoint = validate_segment(args.embedding_endpoint.strip())
    args.chat_endpoint = validate_segment(args.chat_endpoint.strip())
    args.question = args.question.strip()
    if not 3 <= len(args.question) <= 1000:
        parser.error("--question must be 3 to 1000 characters")
    args.symbol = args.symbol.strip().upper() or None
    if args.symbol and not _SYMBOL.fullmatch(args.symbol):
        parser.error(f"invalid --symbol {args.symbol!r}")
    args.retrieval_run = args.retrieval_run.strip()
    if args.retrieval_run:
        validate_segment(args.retrieval_run)
    args.as_of = parse_instant(args.as_of) if args.as_of.strip() else datetime.now(UTC)
    return args


def parse_instant(value: str) -> datetime:
    at = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    return at.astimezone(UTC)


def tuned_run_query(catalog: str, run: str) -> tuple[str, dict]:
    chosen = "AND retrieval_run_id = :run " if run else ""
    return (
        "SELECT retrieval_run_id, adapter_sha256, mlflow_run_id, embedding_model "
        f"FROM {table_fqn(catalog, 'control', RETRIEVAL_RUNS)} "
        f"WHERE retriever = '{TUNED}' AND replay_of IS NULL {chosen}"
        "ORDER BY created_at DESC LIMIT 1",
        {"run": run} if run else {},
    )


def answer_row(args, at, run, sha, hits, answer, mlflow_run_id) -> tuple:
    return (
        uuid.uuid4().hex,
        at,
        args.question,
        args.as_of,
        args.symbol,
        run,
        sha,
        vector_index(args.catalog),
        [
            (
                h["chunk_id"],
                h["accession_number"],
                h["form"],
                h["knowable_at_us"],
                h["score"],
            )
            for h in hits
        ],
        answer,
        args.embedding_endpoint,
        args.chat_endpoint,
        mlflow_run_id,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import mlflow
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for ddl in rag_ddls(args.catalog):
        spark.sql(ddl)
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(args.experiment)
    client = WorkspaceClient()
    at = datetime.now(UTC)

    query, params = tuned_run_query(args.catalog, args.retrieval_run)
    found = spark.sql(query, args=params).collect()
    if not found:
        raise RuntimeError("no tuned retrieval run recorded yet; run the rag job first")
    run = found[0]
    if run["embedding_model"] != args.embedding_endpoint:
        raise RuntimeError(
            f"retrieval run {run['retrieval_run_id']} was trained on {run['embedding_model']}, "
            f"not {args.embedding_endpoint}"
        )
    adapter = load_adapter(mlflow, run["mlflow_run_id"], run["adapter_sha256"])

    base = normalize_rows(as_matrix(Embedder(client, args.embedding_endpoint)([args.question])))
    vector = adapter.apply(base)[0]
    as_of_us = micros(args.as_of)
    hits = query_index(client, vector_index(args.catalog), vector, as_of_us, args.symbol, EXCERPTS)
    excerpts = [Excerpt(h["chunk_id"], h["context"], h["text"]) for h in hits]
    messages = prompt_messages(args.question, args.as_of, excerpts)
    answer = chat(client, args.chat_endpoint, messages)

    with mlflow.start_run(run_name=f"ask {args.run_id}") as logged:
        mlflow.log_params(
            {
                "question": args.question[:500],
                "as_of": args.as_of.isoformat(),
                "symbol": args.symbol or "any",
                "retrieval_run_id": run["retrieval_run_id"],
                "chat_endpoint": args.chat_endpoint,
            }
        )
        mlflow.log_dict({"messages": messages, "answer": answer, "hits": hits}, "answer.json")
        row = answer_row(
            args,
            at,
            run["retrieval_run_id"],
            run["adapter_sha256"],
            hits,
            answer,
            logged.info.run_id,
        )
    spark.createDataFrame([row], schema=RAG_ANSWERS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", RAG_ANSWERS)
    )

    print(f"Question (as of {args.as_of.isoformat()}): {args.question}\n")
    print(answer)
    print("\nSources:")
    used = set(citations(answer, len(excerpts)))
    for n, hit in enumerate(hits, start=1):
        mark = "*" if n in used else " "
        print(f"{mark}[{n}] {hit['context']} (score {hit['score']:.4f})")
    print(json.dumps({"retrieved": len(hits), "cited": sorted(used)}))


if __name__ == "__main__":
    main()

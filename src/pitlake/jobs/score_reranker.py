import argparse
import json
import uuid
from datetime import UTC, datetime

from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.generator import (
    ADAPTER_DIR,
    BASE,
    LORA,
    MANIFEST,
    TRAINING,
    file_sha256,
    read_jsonl,
    score,
)
from pitlake.jobs.tune_retriever import run_row
from pitlake.questions import TEST
from pitlake.rag import EXCERPTS, chat, prompt_messages
from pitlake.reranker import (
    CANDIDATES,
    RERANKER,
    RERANKINGS,
    TUNED_ORDER,
    leaked,
    read_rerankings,
    rerank_metrics,
    reranker_folder,
    top_excerpts,
    tuned_orders,
)
from pitlake.tables import RETRIEVAL_RUNS, RETRIEVAL_RUNS_SCHEMA, rag_ddls


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--chat-endpoint", default="")
    parser.add_argument("--answer-sample", default="0", help="0 asks every test question")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.chat_endpoint = args.chat_endpoint.strip()
    if args.chat_endpoint:
        validate_segment(args.chat_endpoint)
    args.answer_sample = int(args.answer_sample or 0)
    return args


def answer_examples(rows: list[dict], orders: dict[str, list[str]]) -> list[dict]:
    found = []
    for row in rows:
        excerpts = top_excerpts(row, orders[row["question_id"]], EXCERPTS)
        relevant = set(row["relevant"])
        found.append(
            {
                "question_id": row["question_id"],
                "messages": prompt_messages(
                    row["text"], datetime.fromisoformat(row["as_of"]), excerpts
                ),
                "answers": row["answers"],
                "excerpts": [e.chunk_id for e in excerpts],
                "relevant": [e.chunk_id for e in excerpts if e.chunk_id in relevant],
            }
        )
    return found


def scored(rows: list[dict], orders: dict[str, dict[str, list[str]]]) -> dict[str, float]:
    return {
        f"{name}_{k}": v
        for name, given in orders.items()
        for k, v in rerank_metrics(rows, given).items()
    }


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
    at = datetime.now(UTC)

    folder = reranker_folder(volume_path(args.catalog, "gold", STAGING_VOLUME), args.run_id)
    manifest = json.loads((folder / MANIFEST).read_text())
    training = json.loads((folder / TRAINING).read_text())
    rows = read_jsonl(folder / f"{TEST}.jsonl")
    future = sum(leaked(r) for r in rows)
    if future:
        raise RuntimeError(f"{future} test candidates were not knowable when asked")
    reranked = read_rerankings(folder / RERANKINGS)
    orders = {TUNED_ORDER: tuned_orders(rows)}
    for name in (BASE, LORA):
        missing = {r["question_id"] for r in rows} - reranked.get(name, {}).keys()
        if missing:
            raise RuntimeError(f"{name} did not rerank {len(missing)} test questions")
        orders[name] = reranked[name]
    found = scored(rows, orders)

    if args.chat_endpoint:
        client = WorkspaceClient()
        sample = rows[: args.answer_sample] if args.answer_sample else rows
        for name in (TUNED_ORDER, LORA):
            examples = answer_examples(sample, orders[name])
            answers = {
                e["question_id"]: chat(client, args.chat_endpoint, e["messages"]) for e in examples
            }
            found.update({f"{name}_answer_{k}": v for k, v in score(examples, answers).items()})

    model = f"{training['base_model']}/{training['base_model_version']}"
    config = json.dumps(
        {
            **training["config"],
            "base_model": model,
            "best_epoch": training["best_epoch"],
            "candidates": CANDIDATES,
            "retrieval_run_id": manifest["retrieval_run_id"],
            "answer_endpoint": args.chat_endpoint or None,
        },
        sort_keys=True,
    )
    adapter_sha = file_sha256(folder / ADAPTER_DIR / "adapter_model.safetensors")
    rankings_sha = file_sha256(folder / RERANKINGS)
    recorded = []
    with mlflow.start_run(run_name=f"{RERANKER} {args.run_id}") as run:
        mlflow.set_tags(
            {
                "catalog": args.catalog,
                "reranker_run_id": args.run_id,
                "gpu_mlflow_run_id": training["mlflow_run_id"],
                "code_commit": args.code_commit or "unknown",
            }
        )
        mlflow.log_params(
            {
                **{f"lora.{k}": str(v) for k, v in training["config"].items()},
                "base_model": model,
                "best_epoch": training["best_epoch"],
                "retrieval_run_id": manifest["retrieval_run_id"],
                "answer_endpoint": args.chat_endpoint or "none",
                **{f"questions.{s}": n for s, n in manifest["questions"].items()},
            }
        )
        mlflow.log_metrics({f"{RERANKER}.{k}": v for k, v in found.items()})
        mlflow.log_metrics(
            {
                f"candidates.{name}.{k}": v
                for name, m in manifest["candidate_sources"][TEST].items()
                for k, v in m.items()
            }
        )
        mlflow.log_dict(training, "training.json")
        mlflow.log_artifact(str(folder / RERANKINGS))
        mlflow.log_artifacts(str(folder / ADAPTER_DIR), ADAPTER_DIR)
        for name in (BASE, LORA):
            own = {
                k.removeprefix(f"{name}_"): v for k, v in found.items() if k.startswith(f"{name}_")
            }
            baseline = {k: v for k, v in found.items() if k.startswith(f"{TUNED_ORDER}_")}
            recorded.append(
                run_row(
                    args,
                    at,
                    f"rerank_{name}",
                    TEST,
                    True,
                    {**own, **baseline},
                    rankings_sha,
                    adapter_sha if name == LORA else None,
                    config,
                    manifest["table_versions"],
                    model,
                    args.experiment,
                    run.info.run_id,
                )
            )
    spark.createDataFrame(recorded, schema=RETRIEVAL_RUNS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", RETRIEVAL_RUNS)
    )

    for name in orders:
        print(
            f"{name:6} hit@1 {found[f'{name}_hit_at_1']:.3f} hit@5 {found[f'{name}_hit_at_5']:.3f} "
            f"MRR@10 {found[f'{name}_mrr_at_10']:.3f} future {int(found[f'{name}_future_chunks'])}"
        )
    for name in (TUNED_ORDER, LORA):
        if f"{name}_answer_correct" in found:
            print(
                f"{name:6} answers: correct {found[f'{name}_answer_correct']:.3f} "
                f"cited {found[f'{name}_answer_cited_relevant']:.3f}"
            )
    print(
        f"candidates hold a relevant passage for {found[f'{TUNED_ORDER}_hit_at_{CANDIDATES}']:.3f}"
        f" of {len(rows)} questions"
    )
    print(f"OK: recorded reranker run {args.run_id} (rerankings {rankings_sha[:12]})")


if __name__ == "__main__":
    main()

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
    GENERATOR,
    LORA,
    MANIFEST,
    PREDICTIONS,
    REFERENCE,
    TRAINING,
    file_sha256,
    generator_folder,
    read_jsonl,
    score,
)
from pitlake.jobs.tune_retriever import run_row
from pitlake.questions import TEST
from pitlake.rag import chat
from pitlake.tables import RETRIEVAL_RUNS, RETRIEVAL_RUNS_SCHEMA, rag_ddls


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--chat-endpoint", default="")
    parser.add_argument("--reference-sample", default="0", help="0 asks every test question")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.chat_endpoint = args.chat_endpoint.strip()
    if args.chat_endpoint:
        validate_segment(args.chat_endpoint)
    args.reference_sample = int(args.reference_sample or 0)
    return args


def by_model(predictions: list[dict]) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for p in predictions:
        found.setdefault(p["model"], {})[p["question_id"]] = p["answer"]
    return found


def scored(examples: list[dict], answers: dict[str, dict[str, str]]) -> dict[str, float]:
    found = {}
    for name, given in answers.items():
        asked = [e for e in examples if e["question_id"] in given]
        found.update({f"{name}_{k}": v for k, v in score(asked, given).items()})
    return found


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

    folder = generator_folder(volume_path(args.catalog, "gold", STAGING_VOLUME), args.run_id)
    manifest = json.loads((folder / MANIFEST).read_text())
    training = json.loads((folder / TRAINING).read_text())
    examples = read_jsonl(folder / f"{TEST}.jsonl")
    answers = by_model(read_jsonl(folder / PREDICTIONS))
    for name in (BASE, LORA):
        missing = {e["question_id"] for e in examples} - answers.get(name, {}).keys()
        if missing:
            raise RuntimeError(f"{name} did not answer {len(missing)} test questions")

    if args.chat_endpoint:
        client = WorkspaceClient()
        sample = examples[: args.reference_sample] if args.reference_sample else examples
        answers[REFERENCE] = {
            e["question_id"]: chat(client, args.chat_endpoint, e["messages"]) for e in sample
        }
    found = scored(examples, answers)

    model = f"{training['base_model']}/{training['base_model_version']}"
    config = json.dumps(
        {
            **training["config"],
            "base_model": model,
            "best_epoch": training["best_epoch"],
            "retrieval_run_id": manifest["retrieval_run_id"],
            "reference": args.chat_endpoint or None,
        },
        sort_keys=True,
    )
    adapter_sha = file_sha256(folder / ADAPTER_DIR / "adapter_model.safetensors")
    samples = [
        {
            "question": e["messages"][-1]["content"].rsplit("Question: ", 1)[-1],
            "expected": e["answers"],
            "target": e["target"],
            **{name: given.get(e["question_id"]) for name, given in answers.items()},
        }
        for e in examples
    ]
    with mlflow.start_run(run_name=f"generator {args.run_id}") as run:
        mlflow.set_tags(
            {
                "catalog": args.catalog,
                "generator_run_id": args.run_id,
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
                "reference_endpoint": args.chat_endpoint or "none",
                **{f"examples.{s}": n for s, n in manifest["examples"].items()},
            }
        )
        mlflow.log_metrics({f"{GENERATOR}.{k}": v for k, v in found.items()})
        mlflow.log_dict({"answers": samples}, "answers.json")
        mlflow.log_dict(training, "training.json")
        mlflow.log_artifacts(str(folder / ADAPTER_DIR), ADAPTER_DIR)
        row = run_row(
            args,
            at,
            GENERATOR,
            TEST,
            True,
            found,
            file_sha256(folder / PREDICTIONS),
            adapter_sha,
            config,
            manifest["table_versions"],
            model,
            args.experiment,
            run.info.run_id,
        )
    spark.createDataFrame([row], schema=RETRIEVAL_RUNS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", RETRIEVAL_RUNS)
    )

    for name in answers:
        print(
            f"{name:9} accuracy {found[f'{name}_accuracy']:.3f} "
            f"cited {found[f'{name}_cited_relevant']:.3f} "
            f"abstained unanswerable {found[f'{name}_abstained_when_unanswerable']:.3f} "
            f"answerable {found[f'{name}_abstained_when_answerable']:.3f} "
            f"({int(found[f'{name}_questions'])} questions)"
        )
    print(f"OK: recorded generator run {args.run_id} (adapter {adapter_sha[:12]})")


if __name__ == "__main__":
    main()

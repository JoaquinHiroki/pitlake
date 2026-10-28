import argparse
import json
import shutil
import uuid
from datetime import UTC, datetime

from pitlake.config import STAGING_VOLUME, validate_identifier, validate_segment, volume_path
from pitlake.generator import MANIFEST, write_jsonl
from pitlake.jobs.prepare_generator import tuned_rankings
from pitlake.questions import SPLITS
from pitlake.reranker import CANDIDATES, answerable, candidate_row, leaked, reranker_folder


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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import mlflow
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    mlflow.set_tracking_uri("databricks")

    run, versions, corpus, chunks, asked, hits = tuned_rankings(
        spark, mlflow, args.catalog, args.retrieval_run, CANDIDATES
    )
    rows = {s: [] for s in SPLITS}
    for item, ranking in zip(asked, hits, strict=True):
        found = [
            (
                corpus.ids[h.index],
                chunks[corpus.ids[h.index]].context,
                chunks[corpus.ids[h.index]].text,
                corpus.knowable_us[h.index],
            )
            for h in ranking.hits
        ]
        rows[item.split].append(candidate_row(item, found))
    future = sum(leaked(r) for split in rows.values() for r in split)
    if future:
        raise RuntimeError(f"{future} candidate passages were not knowable when asked")

    folder = reranker_folder(volume_path(args.catalog, "gold", STAGING_VOLUME), args.run_id)
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    counts = {s: write_jsonl(folder / f"{s}.jsonl", split) for s, split in rows.items()}
    manifest = {
        "run_id": args.run_id,
        "catalog": args.catalog,
        "prepared_at": datetime.now(UTC).isoformat(),
        "retrieval_run_id": run["retrieval_run_id"],
        "adapter_sha256": run["adapter_sha256"],
        "embedding_model": run["embedding_model"],
        "table_versions": versions,
        "candidates": CANDIDATES,
        "questions": counts,
        "answerable": {s: sum(answerable(r) for r in split) for s, split in rows.items()},
    }
    (folder / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, indent=2, sort_keys=True))
    if not all(manifest["answerable"].values()):
        raise RuntimeError(f"every split needs a question with a relevant candidate: {manifest}")
    print(f"OK: wrote the top {CANDIDATES} candidates of {sum(counts.values())} questions")


if __name__ == "__main__":
    main()

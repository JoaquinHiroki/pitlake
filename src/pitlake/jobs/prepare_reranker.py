import argparse
import json
import shutil
import uuid
from datetime import UTC, datetime

from pitlake.config import STAGING_VOLUME, validate_identifier, validate_segment, volume_path
from pitlake.generator import MANIFEST, write_jsonl
from pitlake.jobs.prepare_generator import tuned_rankings
from pitlake.lexical import Lexicon, fuse, top
from pitlake.questions import SPLITS
from pitlake.rag import embedding_text, micros
from pitlake.reranker import (
    CANDIDATES,
    DENSE,
    HYBRID,
    LEXICAL,
    POOL,
    answerable,
    candidate_metrics,
    candidate_row,
    leaked,
    reranker_folder,
)


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
        spark, mlflow, args.catalog, args.retrieval_run, POOL
    )
    lexicon = Lexicon.build([embedding_text(chunks[c].context, chunks[c].text) for c in corpus.ids])
    rows = {s: [] for s in SPLITS}
    sources = {s: {DENSE: [], LEXICAL: [], HYBRID: []} for s in SPLITS}
    for item, ranking in zip(asked, hits, strict=True):
        as_of_us = micros(item.as_of)
        mask = corpus.visible(as_of_us, item.symbol)
        dense = [h.index for h in ranking.hits]
        lexical = top(lexicon.scores(item.text, mask), mask, POOL)
        ranked = {DENSE: dense, LEXICAL: lexical, HYBRID: fuse([dense, lexical], CANDIDATES)}
        if any(corpus.knowable_us[i] > as_of_us for found in ranked.values() for i in found):
            raise RuntimeError(f"{item.question_id}: a ranked passage was not knowable when asked")
        for name, order in ranked.items():
            ids = [corpus.ids[i] for i in order]
            sources[item.split][name].append((item.question_id, ids, item.relevant))
        found = [
            (corpus.ids[i], chunks[corpus.ids[i]].context, chunks[corpus.ids[i]].text, k)
            for i, k in ((i, corpus.knowable_us[i]) for i in dense[:CANDIDATES])
        ]
        rows[item.split].append(candidate_row(item, found))
    future = sum(leaked(r) for split in rows.values() for r in split)
    if future:
        raise RuntimeError(f"{future} candidate passages were not knowable when asked")
    compared = {
        s: {name: candidate_metrics(ranked) for name, ranked in found.items()}
        for s, found in sources.items()
    }

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
        "candidate_source": DENSE,
        "candidate_sources": compared,
        "questions": counts,
        "answerable": {s: sum(answerable(r) for r in split) for s, split in rows.items()},
    }
    (folder / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, indent=2, sort_keys=True))
    for split, found in compared.items():
        for name, m in found.items():
            print(
                f"{split:10} {name:6} hit@1 {m['hit_at_1']:.3f} hit@5 {m['hit_at_5']:.3f} "
                f"MRR@10 {m['mrr_at_10']:.3f} hit@{CANDIDATES} {m[f'hit_at_{CANDIDATES}']:.3f}"
            )
    if not all(manifest["answerable"].values()):
        raise RuntimeError(f"every split needs a question with a relevant candidate: {manifest}")
    print(f"OK: wrote the top {CANDIDATES} candidates of {sum(counts.values())} questions")


if __name__ == "__main__":
    main()

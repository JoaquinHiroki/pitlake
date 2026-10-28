import argparse
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient
from export_backtest_inputs import WAREHOUSE, query

from pitlake.config import STAGING_VOLUME, table_fqn, validate_identifier, volume_path
from pitlake.jobs.tune_retriever import EMBEDDINGS_KEY, QUESTIONS_KEY
from pitlake.questions import TEST
from pitlake.reranker import RERANKER, RERANKINGS
from pitlake.tables import CHUNK_EMBEDDINGS, RAG_QUESTIONS, RETRIEVAL_RUNS

RERANKED = "retriever IN ('rerank_base', 'rerank_lora')"


def tsv(rows) -> str:
    return "".join("\t".join("" if v is None else str(v) for v in row) + "\n" for row in rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog")
    parser.add_argument("run_id")
    parser.add_argument("out_dir")
    args = parser.parse_args()
    catalog = validate_identifier(args.catalog)
    run = args.run_id
    if not re.fullmatch(r"[0-9]+", run):
        raise SystemExit(f"not a reranker job run id: {run!r}")

    w = WorkspaceClient()
    warehouse_id = next(wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE)
    runs = table_fqn(catalog, "control", RETRIEVAL_RUNS)
    recorded = query(
        w,
        warehouse_id,
        "SELECT retriever, rankings_hash, "
        f"element_at(table_versions, '{QUESTIONS_KEY}'), "
        f"element_at(table_versions, '{EMBEDDINGS_KEY}') "
        f"FROM {runs} WHERE retrieval_run_id = '{run}' AND {RERANKED}",
    )
    if not recorded:
        raise SystemExit(f"no reranker run {run} in {catalog}")
    _, rankings_hash, questions_version, embeddings_version = recorded[0]
    metrics = query(
        w,
        warehouse_id,
        "SELECT substring(retriever, 8), key, CAST(value AS STRING) "
        f"FROM {runs} LATERAL VIEW explode(metrics) m AS key, value "
        f"WHERE retrieval_run_id = '{run}' AND {RERANKED} "
        "AND key NOT LIKE '%answer%' AND key NOT LIKE 'tuned%'",
    )
    questions = query(
        w,
        warehouse_id,
        "SELECT question_id, symbol, as_of_us, array_join(relevant, ',') "
        f"FROM {table_fqn(catalog, 'gold', RAG_QUESTIONS)} VERSION AS OF {int(questions_version)} "
        f"WHERE split = '{TEST}'",
    )
    chunks = query(
        w,
        warehouse_id,
        "SELECT chunk_id, symbol, knowable_at_us "
        f"FROM {table_fqn(catalog, 'gold', CHUNK_EMBEDDINGS)} "
        f"VERSION AS OF {int(embeddings_version)}",
    )
    source = f"{volume_path(catalog, 'gold', STAGING_VOLUME)}/{RERANKER}/{run}/{RERANKINGS}"
    rerankings = w.files.download(source).contents.read()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    meta = {
        "run_id": run,
        "rankings_hash": rankings_hash,
        "questions_version": questions_version,
        "embeddings_version": embeddings_version,
    }
    (out / "meta.tsv").write_text(tsv(meta.items()))
    (out / "metrics.tsv").write_text(tsv(metrics))
    (out / "questions.tsv").write_text(tsv(questions))
    (out / "chunks.tsv").write_text(tsv(chunks))
    (out / RERANKINGS).write_bytes(rerankings)
    print(
        f"wrote {len(questions)} test questions of {RAG_QUESTIONS} version {questions_version}, "
        f"{len(chunks)} chunks of version {embeddings_version} and the rerankings of run {run} "
        f"to {out}"
    )


if __name__ == "__main__":
    main()

import argparse
import json
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path

from pitlake.bronze import parse_optional_date
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.jobs.research import package_version
from pitlake.pit import latest_gold_commits_query
from pitlake.questions import SPLITS, TEST, TRAIN, VALIDATION, Fact, Passage, build_questions
from pitlake.rag import (
    EXCERPTS,
    Embedder,
    Excerpt,
    answered,
    chat,
    citations,
    instant,
    latest_version,
    micros,
    prompt_messages,
    query_index,
    vector_index,
)
from pitlake.retrieval import (
    TOP_K,
    Adapter,
    Corpus,
    Query,
    TrainConfig,
    as_matrix,
    l2_score,
    metrics,
    normalize_rows,
    rank,
    rankings_fingerprint,
    recall,
    train_adapter,
)
from pitlake.snapshots import readable_version
from pitlake.tables import (
    CHUNK_EMBEDDINGS,
    FILING_CHUNKS,
    FUNDAMENTALS,
    RAG_QUESTIONS,
    RAG_QUESTIONS_SCHEMA,
    RETRIEVAL_RUNS,
    RETRIEVAL_RUNS_SCHEMA,
    rag_ddls,
)

BASE = "base"
TUNED = "tuned"
VECTOR_INDEX = "vector_index"
ANSWERS = "rag_answers"
REPRODUCIBLE = (BASE, TUNED)
ADAPTER_FILE = "adapter.npz"
EMBEDDINGS_KEY = f"gold.{CHUNK_EMBEDDINGS}"
QUESTIONS_KEY = f"gold.{RAG_QUESTIONS}"
FUNDAMENTALS_KEY = f"gold.{FUNDAMENTALS}"


class RetrievalLeak(Exception):
    def __init__(self, problems: list[str], rows: list[tuple]):
        self.problems = problems
        self.rows = rows
        super().__init__(f"retrieval leaked the future: {'; '.join(problems)}")


class ReproductionFailed(Exception):
    def __init__(self, failures: dict[str, list[str]], rows: list[tuple]):
        self.failures = failures
        self.rows = rows
        detail = "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(failures.items()))
        super().__init__(f"{len(failures)} evaluations did not reproduce: {detail}")


def _midnight(value: str, name: str, parser) -> datetime:
    day = parse_optional_date(value)
    if day is None:
        parser.error(f"--{name} is required")
    return datetime.combine(day, time(), tzinfo=UTC)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--embedding-endpoint", required=True)
    parser.add_argument("--chat-endpoint", default="")
    parser.add_argument("--train-until", default="")
    parser.add_argument("--test-from", default="")
    parser.add_argument("--rank", default="16")
    parser.add_argument("--epochs", default="150")
    parser.add_argument("--ann-sample", default="100")
    parser.add_argument("--answer-sample", default="0")
    parser.add_argument("--replay", default="", help="A retrieval run ID to reproduce instead")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.embedding_endpoint = validate_segment(args.embedding_endpoint.strip())
    args.chat_endpoint = args.chat_endpoint.strip()
    if args.chat_endpoint:
        validate_segment(args.chat_endpoint)
    args.replay = args.replay.strip()
    if args.replay:
        validate_segment(args.replay)
        return args
    args.train_until = _midnight(args.train_until, "train-until", parser)
    args.test_from = _midnight(args.test_from, "test-from", parser)
    if args.test_from <= args.train_until:
        parser.error("--test-from must be after --train-until")
    args.config = TrainConfig(rank=int(args.rank), epochs=int(args.epochs))
    args.ann_sample = int(args.ann_sample or 0)
    args.answer_sample = int(args.answer_sample or 0)
    return args


@dataclass(frozen=True)
class Chunk:
    accession_number: str
    context: str
    text: str


@dataclass(frozen=True)
class EvalSet:
    splits: dict[str, list[Query]]
    texts: dict[str, str]
    answers: dict[str, tuple[str, ...]]
    accessions: dict[str, str]

    def counts(self) -> dict[str, int]:
        return {s: len(q) for s, q in self.splits.items()}


def eval_set(rows) -> EvalSet:
    rows = sorted(rows, key=lambda r: r["question_id"])
    if not rows:
        raise RuntimeError("no questions to evaluate")
    vectors = normalize_rows(as_matrix([r["embedding"] for r in rows]))
    splits: dict[str, list[Query]] = {s: [] for s in SPLITS}
    for row, vector in zip(rows, vectors, strict=True):
        splits[row["split"]].append(
            Query(
                row["question_id"],
                vector,
                int(row["as_of_us"]),
                row["symbol"],
                frozenset(row["relevant"]),
            )
        )
    return EvalSet(
        splits,
        {r["question_id"]: r["text"] for r in rows},
        {r["question_id"]: tuple(r["answers"]) for r in rows},
        {r["question_id"]: r["accession_number"] for r in rows},
    )


def evaluate_all(corpus: Corpus, sets: EvalSet, adapter: Adapter | None) -> dict:
    results = {}
    retrievers = [(BASE, None)] + ([(TUNED, adapter)] if adapter is not None else [])
    for name, chosen in retrievers:
        for split, queries in sets.splits.items():
            if not queries:
                continue
            rankings = rank(corpus, queries, chosen)
            results[(name, split, True)] = (
                metrics(rankings),
                rankings_fingerprint(corpus, rankings),
            )
        if sets.splits[TEST]:
            rankings = rank(corpus, sets.splits[TEST], chosen, point_in_time=False)
            results[(name, TEST, False)] = (
                metrics(rankings),
                rankings_fingerprint(corpus, rankings),
            )
    return results


def leaks(results: dict) -> list[str]:
    return [
        f"{name} {split}: {int(m['future_chunks'])} chunks"
        for (name, split, pit), (m, _) in sorted(results.items())
        if pit and m["future_chunks"]
    ]


def run_row(
    args,
    at,
    retriever,
    split,
    pit,
    found,
    digest,
    adapter_sha,
    config,
    versions,
    model,
    experiment,
    mlflow_run_id,
    replay_of=None,
    reproduced=None,
) -> tuple:
    return (
        args.run_id,
        at,
        retriever,
        split,
        pit,
        {k: float(v) for k, v in found.items()},
        digest,
        adapter_sha,
        config,
        {k: int(v) for k, v in versions.items()},
        model,
        args.code_commit,
        package_version(),
        experiment,
        mlflow_run_id,
        replay_of,
        reproduced,
    )


def load_corpus(spark, catalog: str, version: int) -> tuple[Corpus, dict[str, Chunk]]:
    frame = spark.sql(
        "SELECT chunk_id, embedding, knowable_at_us, symbol, accession_number, context, text "
        f"FROM {table_fqn(catalog, 'gold', CHUNK_EMBEDDINGS)} VERSION AS OF {int(version)}"
    ).toPandas()
    if frame.empty:
        raise RuntimeError(f"{EMBEDDINGS_KEY} version {version} is empty")
    corpus = Corpus.build(
        list(zip(frame.chunk_id, frame.embedding, frame.knowable_at_us, frame.symbol, strict=True))
    )
    chunks = {
        c: Chunk(a, x, t)
        for c, a, x, t in zip(
            frame.chunk_id, frame.accession_number, frame.context, frame.text, strict=True
        )
    }
    return corpus, chunks


def load_facts(spark, catalog: str, version: int) -> list[Fact]:
    rows = spark.sql(
        "SELECT symbol, metric, duration, period_end, CAST(value AS STRING) AS value, unit, "
        "accession_number, unix_micros(knowable_at) AS knowable_us "
        f"FROM {table_fqn(catalog, 'gold', FUNDAMENTALS)} VERSION AS OF {int(version)} "
        "WHERE value IS NOT NULL AND knowable_at IS NOT NULL"
    ).collect()
    return [
        Fact(
            r["symbol"],
            r["metric"],
            r["duration"],
            r["period_end"],
            Decimal(r["value"]),
            r["unit"],
            r["accession_number"],
            instant(r["knowable_us"]),
        )
        for r in rows
    ]


def committed_fundamentals(spark, catalog: str) -> int:
    commits = {r["gold_table"]: r for r in spark.sql(latest_gold_commits_query(catalog)).collect()}
    if FUNDAMENTALS not in commits:
        raise RuntimeError(f"{catalog} has no committed {FUNDAMENTALS_KEY} yet")
    table = table_fqn(catalog, "gold", FUNDAMENTALS)
    return readable_version(spark, table, commits[FUNDAMENTALS]["gold_version"])


def question_rows(questions, vectors: dict, model: str, run_id: str, at: datetime) -> list[tuple]:
    return [
        (
            q.question_id,
            q.split,
            q.symbol,
            q.metric,
            q.duration,
            q.period_end,
            q.value,
            q.accession_number,
            q.as_of,
            micros(q.as_of),
            q.text,
            list(q.answers),
            list(q.relevant),
            [float(x) for x in vectors[q.text]],
            model,
            at,
            run_id,
        )
        for q in questions
    ]


def read_questions(spark, catalog: str, version: int) -> list[dict]:
    return (
        spark.sql(
            "SELECT question_id, split, symbol, accession_number, as_of_us, text, answers, "
            f"relevant, embedding FROM {table_fqn(catalog, 'gold', RAG_QUESTIONS)} "
            f"VERSION AS OF {int(version)}"
        )
        .toPandas()
        .to_dict("records")
    )


def log_adapter(mlflow, adapter: Adapter) -> None:
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / ADAPTER_FILE
        path.write_bytes(adapter.to_bytes())
        mlflow.log_artifact(str(path))


def load_adapter(mlflow, run_id: str, sha256: str) -> Adapter:
    path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=ADAPTER_FILE)
    adapter = Adapter.from_bytes(Path(path).read_bytes())
    if adapter.fingerprint != sha256:
        raise RuntimeError(f"the adapter of MLflow run {run_id} is not {sha256}")
    return adapter


def ann_check(client, index, corpus, sets, queries, adapter) -> dict[str, float]:
    position = corpus.index()
    recalls, errors, before = [], [], 0
    for query in queries:
        vector = adapter.apply(query.vector[None, :])[0]
        exact = [corpus.ids[h.index] for h in rank(corpus, [query], adapter)[0].hits]
        found = query_index(client, index, vector, query.as_of_us, query.symbol, TOP_K)
        recalls.append(recall(exact, [r["chunk_id"] for r in found]))
        for row in found:
            if row["chunk_id"] not in position:
                raise RuntimeError(
                    f"{index} returned {row['chunk_id']}, which is not in the corpus"
                )
            doc = corpus.vectors[position[row["chunk_id"]]]
            errors.append(abs(row["score"] - float(l2_score(((vector - doc) ** 2).sum()))))
        accession = sets.accessions[query.id]
        earlier = query_index(client, index, vector, query.as_of_us - 1, query.symbol, TOP_K)
        before += sum(r["accession_number"] == accession for r in earlier)
    return {
        "recall_at_10": sum(recalls) / len(recalls),
        "min_recall_at_10": min(recalls),
        "score_max_abs_error": max(errors, default=0.0),
        "invisible_before_acceptance_cases": float(before),
        "questions": float(len(queries)),
    }


def answer_check(client, endpoint, corpus, chunks, sets, queries, adapter):
    found, samples = {}, []
    for name, chosen in ((BASE, None), (TUNED, adapter)):
        correct = cited = 0
        for query in queries:
            ids = [corpus.ids[h.index] for h in rank(corpus, [query], chosen, k=EXCERPTS)[0].hits]
            excerpts = [Excerpt(c, chunks[c].context, chunks[c].text) for c in ids]
            messages = prompt_messages(sets.texts[query.id], instant(query.as_of_us), excerpts)
            answer = chat(client, endpoint, messages)
            ok = answered(answer, sets.answers[query.id])
            used = citations(answer, len(excerpts))
            relevant = any(excerpts[n - 1].chunk_id in query.relevant for n in used)
            correct += ok
            cited += relevant
            samples.append(
                {
                    "retriever": name,
                    "question": sets.texts[query.id],
                    "as_of_us": query.as_of_us,
                    "expected": list(sets.answers[query.id]),
                    "answer": answer,
                    "correct": ok,
                    "cited_relevant": relevant,
                    "excerpts": [e.chunk_id for e in excerpts],
                }
            )
        found[f"{name}_accuracy"] = correct / len(queries)
        found[f"{name}_cited_relevant"] = cited / len(queries)
    found["questions"] = float(len(queries))
    return found, samples


def print_results(results: dict) -> None:
    for (name, split, pit), (m, digest) in sorted(results.items()):
        print(
            f"{name:5} {split:10} {'pit' if pit else 'no-pit'}: hit@1 {m['hit_at_1']:.3f} "
            f"hit@5 {m['hit_at_5']:.3f} mrr@10 {m['mrr_at_10']:.3f} "
            f"future {int(m['future_chunks'])} "
            f"({int(m['questions'])} questions, {digest[:12]})"
        )


def tune(spark, mlflow, client, args, at: datetime) -> list[tuple]:
    embeddings_table = table_fqn(args.catalog, "gold", CHUNK_EMBEDDINGS)
    version = latest_version(spark, embeddings_table)
    embedded = spark.sql(f"SELECT count(*) AS n FROM {embeddings_table} VERSION AS OF {version}")
    chunked = spark.table(table_fqn(args.catalog, "gold", FILING_CHUNKS)).count()
    if embedded.collect()[0]["n"] != chunked:
        raise RuntimeError(f"{EMBEDDINGS_KEY} does not cover gold.{FILING_CHUNKS}; embed all first")
    corpus, chunks = load_corpus(spark, args.catalog, version)
    fundamentals_version = committed_fundamentals(spark, args.catalog)
    passages = [
        Passage(c, str(corpus.symbols[i]), chunks[c].accession_number, chunks[c].text)
        for i, c in enumerate(corpus.ids)
    ]
    questions, dropped = build_questions(
        load_facts(spark, args.catalog, fundamentals_version),
        passages,
        args.train_until,
        args.test_from,
    )
    print(
        f"{len(questions)} questions from {FUNDAMENTALS_KEY} v{fundamentals_version}, dropped "
        f"{json.dumps(dict(sorted(dropped.items())))}"
    )

    questions_table = table_fqn(args.catalog, "gold", RAG_QUESTIONS)
    cache = {
        r["text"]: r["embedding"]
        for r in spark.sql(
            f"SELECT text, embedding FROM {questions_table} WHERE embedding_model = :model",
            args={"model": args.embedding_endpoint},
        ).collect()
    }
    missing = sorted({q.text for q in questions} - cache.keys())
    print(f"embedding {len(missing)} new questions ({len(cache)} cached)")
    cache.update(zip(missing, Embedder(client, args.embedding_endpoint)(missing), strict=True))

    from pyspark.sql import functions as F

    spark.createDataFrame(
        question_rows(questions, cache, args.embedding_endpoint, args.run_id, at),
        schema=RAG_QUESTIONS_SCHEMA,
    ).writeTo(questions_table).overwrite(F.lit(True))
    questions_version = latest_version(spark, questions_table)
    sets = eval_set(read_questions(spark, args.catalog, questions_version))
    print(f"questions per split: {sets.counts()}")
    if not (sets.splits[TRAIN] and sets.splits[VALIDATION] and sets.splits[TEST]):
        raise RuntimeError(f"every split needs questions, got {sets.counts()}; move the dates")

    versions = {
        EMBEDDINGS_KEY: version,
        QUESTIONS_KEY: questions_version,
        FUNDAMENTALS_KEY: fundamentals_version,
    }
    config = json.dumps(args.config.to_dict(), sort_keys=True)
    rows, problems = [], []
    with mlflow.start_run(run_name=f"retrieval {args.run_id}") as run:
        mlflow.set_tags(
            {
                "catalog": args.catalog,
                "retrieval_run_id": args.run_id,
                "code_commit": args.code_commit or "unknown",
                "package_version": package_version(),
                **{k: str(v) for k, v in versions.items()},
            }
        )
        mlflow.log_params(
            {
                **{f"train.{k}": str(v) for k, v in args.config.to_dict().items()},
                "train_until": args.train_until.date().isoformat(),
                "test_from": args.test_from.date().isoformat(),
                "embedding_endpoint": args.embedding_endpoint,
                "chat_endpoint": args.chat_endpoint or "none",
                "chunks": len(corpus),
                **{f"questions.{s}": n for s, n in sets.counts().items()},
            }
        )
        mlflow.log_dict(dict(sorted(dropped.items())), "dropped_questions.json")

        def on_epoch(record):
            step = record["epoch"]
            mlflow.log_metric("train.loss", record["loss"], step=step)
            if "validation_mrr_at_10" in record:
                mlflow.log_metric("validation.mrr_at_10", record["validation_mrr_at_10"], step=step)

        adapter, history = train_adapter(
            corpus, sets.splits[TRAIN], sets.splits[VALIDATION], args.config, on_epoch
        )
        mlflow.log_dict({"history": history}, "training_history.json")
        log_adapter(mlflow, adapter)
        print(
            f"trained a rank-{adapter.rank} adapter in {len(history) - 1} epochs, "
            f"{adapter.fingerprint[:12]}"
        )

        results = evaluate_all(corpus, sets, adapter)
        print_results(results)
        problems += leaks(results)
        experiment = args.experiment
        for (name, split, pit), (found, digest) in sorted(results.items()):
            suffix = "" if pit else ".no_pit"
            mlflow.log_metrics({f"{name}.{split}{suffix}.{k}": v for k, v in found.items()})
            rows.append(
                run_row(
                    args,
                    at,
                    name,
                    split,
                    pit,
                    found,
                    digest,
                    adapter.fingerprint if name == TUNED else "",
                    config,
                    versions,
                    args.embedding_endpoint,
                    experiment,
                    run.info.run_id,
                )
            )

        sample = sets.splits[TEST]
        if args.ann_sample:
            checked = ann_check(
                client, vector_index(args.catalog), corpus, sets, sample[: args.ann_sample], adapter
            )
            print(f"vector index vs exact search: {json.dumps(checked)}")
            mlflow.log_metrics({f"{VECTOR_INDEX}.{k}": v for k, v in checked.items()})
            rows.append(
                run_row(
                    args,
                    at,
                    VECTOR_INDEX,
                    TEST,
                    True,
                    checked,
                    "",
                    adapter.fingerprint,
                    config,
                    versions,
                    args.embedding_endpoint,
                    experiment,
                    run.info.run_id,
                )
            )
            if checked["invisible_before_acceptance_cases"]:
                problems.append(
                    f"vector index: {checked['invisible_before_acceptance_cases']:.0f} "
                    "chunks of filings returned before their acceptance"
                )
        if args.chat_endpoint and args.answer_sample:
            answered_, samples = answer_check(
                client,
                args.chat_endpoint,
                corpus,
                chunks,
                sets,
                sample[: args.answer_sample],
                adapter,
            )
            print(f"answers through {args.chat_endpoint}: {json.dumps(answered_)}")
            mlflow.log_metrics({f"{ANSWERS}.{k}": v for k, v in answered_.items()})
            mlflow.log_dict({"answers": samples}, "answers.json")
            rows.append(
                run_row(
                    args,
                    at,
                    ANSWERS,
                    TEST,
                    True,
                    answered_,
                    "",
                    adapter.fingerprint,
                    config,
                    versions,
                    args.chat_endpoint,
                    experiment,
                    run.info.run_id,
                )
            )
    if problems:
        raise RetrievalLeak(problems, rows)
    return rows


def replay(spark, mlflow, args, at: datetime) -> list[tuple]:
    originals = spark.sql(
        f"SELECT * FROM {table_fqn(args.catalog, 'control', RETRIEVAL_RUNS)} "
        "WHERE retrieval_run_id = :id AND replay_of IS NULL AND retriever IN ('base', 'tuned')",
        args={"id": args.replay},
    ).collect()
    if not originals:
        raise RuntimeError(f"no recorded retrieval run {args.replay!r} to replay")
    versions = dict(originals[0]["table_versions"])
    corpus, _ = load_corpus(spark, args.catalog, versions[EMBEDDINGS_KEY])
    sets = eval_set(read_questions(spark, args.catalog, versions[QUESTIONS_KEY]))
    tuned = [o for o in originals if o["retriever"] == TUNED]
    adapter = (
        load_adapter(mlflow, tuned[0]["mlflow_run_id"], tuned[0]["adapter_sha256"])
        if tuned
        else None
    )
    results = evaluate_all(corpus, sets, adapter)
    print_results(results)
    rows, failures = [], {}
    with mlflow.start_run(run_name=f"replay {args.replay}") as run:
        mlflow.set_tags({"replay_of": args.replay, "retrieval_run_id": args.run_id})
        for original in originals:
            key = (original["retriever"], original["split"], original["point_in_time"])
            label = f"{key[0]} {key[1]} {'pit' if key[2] else 'no-pit'}"
            if key not in results:
                failures[label] = ["missing"]
                continue
            found, digest = results[key]
            differs = [] if digest == original["rankings_hash"] else ["rankings"]
            differs += [k for k, v in original["metrics"].items() if found.get(k) != v]
            if differs:
                failures[label] = differs
            print(f"{'IDENTICAL' if not differs else 'DIFFERENT'}: {label} {', '.join(differs)}")
            suffix = "" if key[2] else ".no_pit"
            mlflow.log_metrics({f"{key[0]}.{key[1]}{suffix}.{k}": v for k, v in found.items()})
            rows.append(
                run_row(
                    args,
                    at,
                    key[0],
                    key[1],
                    key[2],
                    found,
                    digest,
                    original["adapter_sha256"],
                    original["train_config"],
                    versions,
                    original["embedding_model"],
                    args.experiment,
                    run.info.run_id,
                    replay_of=args.replay,
                    reproduced=not differs,
                )
            )
    if failures:
        raise ReproductionFailed(failures, rows)
    return rows


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import mlflow
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in rag_ddls(args.catalog):
        spark.sql(ddl)
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(args.experiment)
    at = datetime.now(UTC)

    rows = []
    try:
        if args.replay:
            rows = replay(spark, mlflow, args, at)
        else:
            rows = tune(spark, mlflow, WorkspaceClient(), args, at)
    except (ReproductionFailed, RetrievalLeak) as failed:
        rows = failed.rows
        raise
    finally:
        if rows:
            spark.createDataFrame(rows, schema=RETRIEVAL_RUNS_SCHEMA).write.mode(
                "append"
            ).saveAsTable(table_fqn(args.catalog, "control", RETRIEVAL_RUNS))
    kind = "reproduced" if args.replay else "recorded"
    print(f"OK: {kind} {len(rows)} retrieval evaluations in {args.catalog}")


if __name__ == "__main__":
    main()

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace as NS

import numpy as np
import pytest
from databricks.sdk.errors import NotFound

from pitlake.documents import chunk_id
from pitlake.jobs.ask import parse_args as ask_args
from pitlake.jobs.ask import parse_instant, tuned_run_query
from pitlake.jobs.embed_chunks import COLUMNS, embedded_rows
from pitlake.jobs.tune_retriever import (
    BASE,
    TUNED,
    eval_set,
    evaluate_all,
    leaks,
    question_rows,
    run_row,
)
from pitlake.jobs.tune_retriever import parse_args as tune_args
from pitlake.questions import TEST, TRAIN, VALIDATION, Question
from pitlake.rag import (
    Embedder,
    Excerpt,
    answered,
    chat,
    citations,
    document_chunks,
    ensure_index,
    index_filters,
    instant,
    micros,
    parse_query_response,
    prompt_messages,
    query_index,
    vector_index,
)
from pitlake.retrieval import Adapter, Corpus
from pitlake.tables import (
    CHUNK_EMBEDDINGS_SCHEMA,
    RAG_ANSWERS_SCHEMA,
    RAG_QUESTIONS_SCHEMA,
    RETRIEVAL_RUNS_SCHEMA,
    rag_ddls,
)

AT = datetime(2024, 8, 2, 10, 1, 27, tzinfo=UTC)
HTML = "<html><body><p>Item 2. Management's Discussion</p><p>Net sales rose.</p></body></html>"


def top_level_columns(schema: str) -> list[str]:
    columns, depth, current = [], 0, ""
    for ch in schema:
        depth += ch in "<("
        depth -= ch in ">)"
        if ch == "," and depth == 0:
            columns.append(current.strip())
            current = ""
        else:
            current += ch
    return [*columns, current.strip()]


def test_instants_round_trip_in_microseconds():
    assert instant(micros(AT)) == AT
    assert micros(datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)) == 1_000_000
    with pytest.raises(TypeError):
        micros(datetime(2024, 1, 1))


def test_document_chunks_carry_ids_and_context():
    [(cid, index, section, context, text)] = document_chunks(
        "AAPL", "0000320193-24-000081", "10-Q", date(2024, 6, 29), date(2024, 8, 2), HTML
    )
    assert cid == chunk_id("0000320193-24-000081", 0, text) and index == 0
    assert section == "Item 2. Management's Discussion"
    assert context.startswith("Apple Inc. (AAPL) Form 10-Q for the period ended June 29, 2024")
    assert text == "Item 2. Management's Discussion\nNet sales rose."


class FakeServing:
    def __init__(self, dimension=3, drop=False):
        self.calls = []
        self.dimension = dimension
        self.drop = drop

    def query(self, name, input=None, messages=None, max_tokens=None, temperature=None):
        self.calls.append((name, input, messages, temperature))
        if messages is not None:
            return NS(choices=[NS(message=NS(content="It was 85,777 [1] [7]."))])
        data = [
            NS(index=i, embedding=[float(len(t))] * self.dimension) for i, t in enumerate(input)
        ]
        return NS(data=list(reversed(data))[: len(data) - self.drop])


def test_embedder_batches_and_keeps_order():
    serving = FakeServing()
    vectors = Embedder(NS(serving_endpoints=serving), "gte", batch=2)(["a", "bb", "ccc"])
    assert vectors == [[1.0] * 3, [2.0] * 3, [3.0] * 3]
    assert [c[1] for c in serving.calls] == [["a", "bb"], ["ccc"]]


def test_embedder_refuses_missing_embeddings():
    with pytest.raises(RuntimeError, match="returned 1 embeddings for 2"):
        Embedder(NS(serving_endpoints=FakeServing(drop=True)), "gte")(["a", "b"])


def test_chat_is_deterministic_and_returns_the_message():
    serving = FakeServing()
    answer = chat(NS(serving_endpoints=serving), "llm", [{"role": "user", "content": "q"}])
    assert answer == "It was 85,777 [1] [7]."
    name, _, messages, temperature = serving.calls[0]
    assert (name, temperature, messages[0].content) == ("llm", 0.0, "q")


def test_prompt_numbers_excerpts_and_states_the_instant():
    messages = prompt_messages(
        "Apple revenue?", AT, [Excerpt("c1", "Apple 10-Q", "Total net sales | 85,777")]
    )
    assert messages[0]["role"] == "system" and "August 2, 2024 10:01 UTC" in messages[0]["content"]
    assert "[1] Apple 10-Q\nTotal net sales | 85,777" in messages[1]["content"]
    assert messages[1]["content"].endswith("Question: Apple revenue?")
    assert "(none)" in prompt_messages("q", AT, [])[1]["content"]


def test_citations_and_answers():
    assert citations("See [2] and [1], also [9] and [2].", 3) == [1, 2]
    assert answered("Net sales were $85,777 million [1].", ("85,777", "85,777,000"))
    assert not answered("Net sales were $185,777 million.", ("85,777",))
    assert not answered("anything", ())


def test_index_filters_and_names():
    assert index_filters(5, "AAPL") == {"knowable_at_us <=": 5, "symbol": "AAPL"}
    assert index_filters(5, None) == {"knowable_at_us <=": 5}
    assert vector_index("pitlake_dev") == "pitlake_dev.gold.chunk_embeddings_index"


def response(rows, columns=("chunk_id", "knowable_at_us", "score")):
    return NS(
        manifest=NS(columns=[NS(name=c) for c in columns]),
        result=NS(data_array=rows),
    )


def test_parse_query_response():
    assert parse_query_response(response([["c1", 5, 0.5]])) == [
        {"chunk_id": "c1", "knowable_at_us": 5, "score": 0.5}
    ]
    assert parse_query_response(NS(manifest=NS(columns=[]), result=None)) == []


class FakeIndexes:
    def __init__(self, rows=(), exists=True):
        self.rows = list(rows)
        self.exists = exists
        self.created = []
        self.queries = []

    def query_index(self, **kwargs):
        self.queries.append(kwargs)
        return response(self.rows)

    def get_index(self, name):
        if not self.exists:
            raise NotFound("no index")
        return NS(status=NS(ready=True, indexed_row_count=3))

    def create_index(self, **kwargs):
        self.created.append(kwargs)


def test_query_index_sends_the_point_in_time_filter():
    indexes = FakeIndexes([["c1", "5", "0.5"]])
    rows = query_index(NS(vector_search_indexes=indexes), "i", np.array([0.1, 0.2]), 5, "AAPL", 3)
    assert rows == [{"chunk_id": "c1", "knowable_at_us": 5, "score": 0.5}]
    sent = indexes.queries[0]
    assert sent["filters_json"] == '{"knowable_at_us <=": 5, "symbol": "AAPL"}'
    assert sent["query_vector"] == [0.1, 0.2] and sent["num_results"] == 3


def test_query_index_refuses_a_chunk_from_the_future():
    client = NS(vector_search_indexes=FakeIndexes([["c1", 6, 0.5]]))
    with pytest.raises(RuntimeError, match="point-in-time filter did not hold"):
        query_index(client, "i", [0.1], 5, None, 3)


def test_ensure_index_creates_a_triggered_self_managed_index_once():
    missing = FakeIndexes(exists=False)
    assert ensure_index(NS(vector_search_indexes=missing), "vs", "c.gold.i", "c.gold.t", 1024)
    [spec] = missing.created
    assert (spec["primary_key"], spec["endpoint_name"]) == ("chunk_id", "vs")
    sync = spec["delta_sync_index_spec"]
    assert sync.pipeline_type.value == "TRIGGERED" and sync.source_table == "c.gold.t"
    assert sync.embedding_vector_columns[0].embedding_dimension == 1024
    assert "knowable_at_us" in sync.columns_to_sync
    assert not ensure_index(NS(vector_search_indexes=FakeIndexes()), "vs", "i", "t", 3)


def test_embedded_rows_are_unit_length_and_match_the_schema():
    row = {c: c for c in COLUMNS} | {"knowable_at_us": 7}
    [out] = embedded_rows([row], [[3.0, 4.0]], "gte", "run", AT)
    assert len(out) == len(top_level_columns(CHUNK_EMBEDDINGS_SCHEMA))
    assert out[-4] == [0.6, 0.8] and out[-5] == 7 and out[-3] == "gte"
    assert embedded_rows([], [], "gte", "run", AT) == []


def test_only_the_embedding_table_feeds_change_data_to_the_index():
    gold = [d for d in rag_ddls("pitlake_dev") if "`gold`" in d]
    assert len(gold) == 3 and all("interval 365 days" in d for d in gold)
    assert ["enableChangeDataFeed" in d for d in gold] == [False, True, False]


def test_tune_args():
    common = ["--catalog", "pitlake_dev", "--experiment", "/x", "--embedding-endpoint", "gte"]
    args = tune_args([*common, "--train-until", "2024-01-01", "--test-from", "2025-01-01"])
    assert args.train_until == datetime(2024, 1, 1, tzinfo=UTC)
    assert args.config.rank == 16 and args.answer_sample == 0 and args.ann_sample == 100
    assert tune_args([*common, "--replay", " 42 "]).replay == "42"
    with pytest.raises(SystemExit):
        tune_args([*common, "--train-until", "2025-01-01", "--test-from", "2024-01-01"])
    with pytest.raises(SystemExit):
        tune_args([*common, "--test-from", "2025-01-01"])


def stored(question_id, split, vector, as_of_us, relevant):
    return {
        "question_id": question_id,
        "split": split,
        "symbol": "AAPL",
        "accession_number": "a",
        "as_of_us": as_of_us,
        "text": f"q {question_id}",
        "answers": ["1"],
        "relevant": relevant,
        "embedding": vector,
    }


def test_evaluation_covers_every_split_and_retriever_and_finds_no_leak():
    corpus = Corpus.build(
        [
            ("c0", [1.0, 0.0, 0.0], 10, "AAPL"),
            ("c1", [0.0, 1.0, 0.0], 20, "AAPL"),
            ("c2", [0.0, 0.0, 1.0], 30, "AAPL"),
        ]
    )
    sets = eval_set(
        [
            stored("t", TRAIN, [1.0, 0.1, 0.0], 10, ["c0"]),
            stored("v", VALIDATION, [0.1, 1.0, 0.0], 20, ["c1"]),
            stored("x", TEST, [0.0, 0.1, 1.0], 25, ["c2"]),
        ]
    )
    assert sets.counts() == {TRAIN: 1, VALIDATION: 1, TEST: 1}
    results = evaluate_all(corpus, sets, Adapter.init(3, 1, seed=0))
    assert set(results) == {
        (r, s, True) for r in (BASE, TUNED) for s in (TRAIN, VALIDATION, TEST)
    } | {(BASE, TEST, False), (TUNED, TEST, False)}
    assert results[(BASE, TEST, True)][0]["hit_at_1"] == 0.0
    assert results[(BASE, TEST, False)][0]["future_chunks"] == 1.0
    assert results[(BASE, TRAIN, True)][0]["hit_at_1"] == 1.0
    assert leaks(results) == []
    assert results[(BASE, TEST, True)][1] == results[(TUNED, TEST, True)][1]


def test_rows_match_their_schemas():
    args = tune_args(
        ["--catalog", "c", "--experiment", "/x", "--embedding-endpoint", "gte", "--replay", "1"]
    )
    args.code_commit = "abc"
    row = run_row(
        args, AT, BASE, TEST, True, {"hit_at_1": 1}, "h", "", "{}", {"t": 1}, "gte", "/x", "m"
    )
    assert len(row) == len(top_level_columns(RETRIEVAL_RUNS_SCHEMA))
    question = Question(
        "id",
        TEST,
        "AAPL",
        "revenue",
        "quarter",
        date(2024, 6, 29),
        Decimal("1"),
        "a",
        AT,
        "text",
        ("1",),
        ("c0",),
    )
    [qrow] = question_rows([question], {"text": [0.5]}, "gte", "run", AT)
    assert len(qrow) == len(top_level_columns(RAG_QUESTIONS_SCHEMA))
    assert qrow[9] == micros(AT)
    assert len(top_level_columns(RAG_ANSWERS_SCHEMA)) == 13


def test_ask_args():
    common = [
        "--catalog",
        "pitlake_dev",
        "--experiment",
        "/x",
        "--embedding-endpoint",
        "gte",
        "--chat-endpoint",
        "llm",
    ]
    args = ask_args(
        [
            *common,
            "--question",
            "Apple revenue?",
            "--as-of",
            "2024-08-02T10:01:27Z",
            "--symbol",
            "aapl",
        ]
    )
    assert args.as_of == AT and args.symbol == "AAPL"
    assert ask_args([*common, "--question", "Apple revenue?"]).as_of.tzinfo is UTC
    assert parse_instant("2024-08-02T06:01:27-04:00") == AT
    with pytest.raises(SystemExit):
        ask_args([*common, "--question", "x"])
    with pytest.raises(SystemExit):
        ask_args([*common, "--question", "Apple?", "--symbol", "AA PL"])


def test_tuned_run_query_picks_the_latest_or_the_named_run():
    query, params = tuned_run_query("pitlake_dev", "")
    assert "retriever = 'tuned'" in query and "LIMIT 1" in query and params == {}
    query, params = tuned_run_query("pitlake_dev", "42")
    assert ":run" in query and params == {"run": "42"}

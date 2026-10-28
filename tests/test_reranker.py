import random
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
import yaml

from pitlake.generator import Asked, score
from pitlake.jobs.prepare_reranker import parse_args as prepare_args
from pitlake.jobs.score_reranker import answer_examples, scored
from pitlake.jobs.score_reranker import parse_args as score_args
from pitlake.jobs.train_reranker import DEFAULTS
from pitlake.rag import micros
from pitlake.reranker import (
    CANDIDATES,
    candidate_row,
    judge_messages,
    leaked,
    ordering,
    read_rerankings,
    reorder,
    rerank_metrics,
    training_group,
    tuned_orders,
    write_rerankings,
)

AT = datetime(2024, 8, 2, 10, 1, 27, tzinfo=UTC)


def asked(**changes) -> Asked:
    fields = {
        "question_id": "q1",
        "split": "test",
        "symbol": "AAPL",
        "metric": "revenue",
        "duration": "quarter",
        "period_end": date(2024, 6, 29),
        "value": Decimal("85777000000"),
        "as_of": AT,
        "text": "Report Apple Inc.'s total net sales in the three months ended June 29, 2024.",
        "answers": ("85,777",),
        "relevant": frozenset({"c3"}),
    }
    return Asked(**{**fields, **changes})


def row(relevant=("c3",), count=12, question="q1", late=()) -> dict:
    chunks = [
        (
            f"c{n}",
            "Apple 10-Q",
            "Total net sales | 85,777" if f"c{n}" in relevant else f"Other line {n}",
            micros(AT) + (1 if f"c{n}" in late else -1),
        )
        for n in range(1, count + 1)
    ]
    return candidate_row(asked(question_id=question, relevant=frozenset(relevant)), chunks)


def test_candidate_rows_keep_order_relevance_and_knowable_time():
    found = row()
    assert [c["chunk_id"] for c in found["candidates"]][:3] == ["c1", "c2", "c3"]
    assert [c["relevant"] for c in found["candidates"]][:3] == [False, False, True]
    assert found["as_of_us"] == micros(AT)
    assert found["relevant"] == ["c3"]
    assert leaked(found) == 0
    assert leaked(row(late=("c5", "c9"))) == 2


def test_the_judge_prompt_states_the_question_date_and_the_passage():
    system, user = judge_messages("What were sales?", AT, "Apple 10-Q", "Total net sales | 85,777")
    assert "August 2, 2024 10:01 UTC" in system["content"]
    assert "Apple 10-Q\nTotal net sales | 85,777" in user["content"]
    assert "Question: What were sales?" in user["content"]


def test_a_training_group_is_one_relevant_passage_then_hard_negatives():
    found = row(relevant=("c3", "c7"))
    group = training_group(found, random.Random(0), size=8)
    candidates = found["candidates"]
    assert len(group) == 8 and len(set(group)) == 8
    assert candidates[group[0]]["relevant"]
    assert not any(candidates[i]["relevant"] for i in group[1:])
    assert group == training_group(found, random.Random(0), size=8)
    assert training_group(row(relevant=()), random.Random(0)) == []


def test_reorder_sorts_by_score_and_keeps_the_tuned_order_on_ties():
    assert reorder(["a", "b", "c", "d"], [0.1, 2.0, 0.1, 3.0]) == ["d", "b", "a", "c"]
    with pytest.raises(ValueError, match="one score"):
        reorder(["a"], [1.0, 2.0])


def test_ordering_scores_only_the_top_ten_like_the_retriever():
    found = row(relevant=("c11",))
    tuned = tuned_orders([found])["q1"]
    assert ordering(found, tuned).first_relevant is None
    moved = ["c11", *[c for c in tuned if c != "c11"]]
    assert ordering(found, moved).first_relevant == 1
    with pytest.raises(ValueError, match="not of its candidates"):
        ordering(found, tuned[:-1] + ["elsewhere"])


def test_rerank_metrics_match_the_retriever_definitions():
    rows = [row(question="q1"), row(question="q2", relevant=()), row(question="q3")]
    orders = tuned_orders(rows)
    orders["q3"] = ["c3", *[c for c in orders["q3"] if c != "c3"]]
    found = rerank_metrics(rows, orders)
    assert found["hit_at_1"] == pytest.approx(1 / 3)
    assert found["hit_at_5"] == pytest.approx(2 / 3)
    assert found["mrr_at_10"] == pytest.approx((1 / 3 + 1) / 3)
    assert found[f"hit_at_{CANDIDATES}"] == pytest.approx(2 / 3)
    assert found["future_chunks"] == 0
    assert found["questions"] == 3


def test_a_late_passage_in_the_top_ten_is_counted_as_future():
    found = row(late=("c2",))
    assert rerank_metrics([found], tuned_orders([found]))["future_chunks"] == 1


def test_rerankings_round_trip_in_a_stable_order(tmp_path):
    path = tmp_path / "rerankings.tsv"
    write_rerankings(path, [("q2", "lora", ["b", "a"]), ("q1", "base", ["a", "b"])])
    assert path.read_text() == "q1\tbase\ta,b\nq2\tlora\tb,a\n"
    assert read_rerankings(path) == {"base": {"q1": ["a", "b"]}, "lora": {"q2": ["b", "a"]}}


def test_answer_examples_use_each_order_top_five():
    found = row(relevant=("c9",))
    orders = tuned_orders([found])
    tuned = answer_examples([found], orders)[0]
    assert tuned["excerpts"] == ["c1", "c2", "c3", "c4", "c5"] and tuned["relevant"] == []
    moved = {"q1": ["c9", *[c for c in orders["q1"] if c != "c9"]]}
    reranked = answer_examples([found], moved)[0]
    assert reranked["excerpts"][0] == "c9" and reranked["relevant"] == ["c9"]
    assert "[1] Apple 10-Q\nTotal net sales | 85,777" in reranked["messages"][1]["content"]
    answers = {"q1": "Apple reported total net sales of $85,777 million [1]."}
    assert score([reranked], answers)["correct"] == 1.0
    assert score([tuned], answers)["correct"] == 0.0


def test_scored_prefixes_each_order():
    found = row()
    result = scored([found], {"tuned": tuned_orders([found])})
    assert result["tuned_mrr_at_10"] == pytest.approx(1 / 3)


def test_arguments():
    args = prepare_args(["--catalog", "pitlake_dev", "--run-id", "42"])
    assert (args.catalog, args.run_id, args.retrieval_run) == ("pitlake_dev", "42", "")
    args = score_args(["--catalog", "pitlake_dev", "--experiment", "/x", "--answer-sample", ""])
    assert args.answer_sample == 0 and args.chat_endpoint == ""
    with pytest.raises(ValueError):
        prepare_args(["--catalog", "pitlake dev"])


def test_the_job_parameters_cover_the_gpu_defaults():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["reranker"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert {k: params[k] for k in DEFAULTS} == DEFAULTS
    assert [t["task_key"] for t in job["tasks"]] == ["prepare", "train", "score"]
    assert job["tags"]["catalog"] == "${var.catalog}"

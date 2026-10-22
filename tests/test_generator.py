import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from pitlake.generator import (
    Asked,
    abstained,
    amount,
    example,
    examples_for,
    file_sha256,
    read_jsonl,
    score,
    target_answer,
    withheld,
    write_jsonl,
)
from pitlake.jobs.prepare_generator import parse_args as prepare_args
from pitlake.jobs.score_generator import by_model, scored
from pitlake.jobs.score_generator import parse_args as score_args
from pitlake.rag import Excerpt, moment

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
        "answers": ("85,777", "85,777,000"),
        "relevant": frozenset({"c2"}),
    }
    return Asked(**{**fields, **changes})


EXCERPTS = [
    Excerpt("c1", "Apple 10-Q", "Services net sales were 24,213."),
    Excerpt("c2", "Apple 10-Q", "Total net sales | 85,777 | 81,797"),
]


def test_amount_follows_the_printed_scale_and_unit():
    assert amount(Decimal("85777000000"), "revenue", "85,777") == "$85,777 million"
    assert amount(Decimal("85777000000"), "revenue", "85,777,000") == "$85,777,000 thousand"
    assert amount(Decimal("-1234000000"), "net_income", "1,234") == "-$1,234 million"
    assert amount(Decimal("1.40"), "eps_diluted", "1.40") == "$1.40"
    shares = Decimal("15204137000")
    assert amount(shares, "shares_outstanding", "15,204,137,000") == "15,204,137,000"
    assert amount(shares, "shares_outstanding", "15,204,137") == "15,204,137 thousand"


def test_target_cites_the_first_relevant_excerpt_with_the_value_it_prints():
    text, cited = target_answer(asked(), EXCERPTS)
    assert cited == 2
    assert text == (
        "Apple Inc. reported total net sales of $85,777 million for the three months ended "
        "June 29, 2024 [2]."
    )
    balance = Excerpt("c3", "Apple 10-Q", "Total assets | 331,612 | 352,583")
    text, cited = target_answer(
        asked(
            metric="assets",
            duration="instant",
            value=Decimal("331612000000"),
            answers=("331,612", "331,612,000"),
            relevant=frozenset({"c3"}),
        ),
        [balance],
    )
    assert (text, cited) == (
        "Apple Inc. reported total assets of $331,612 million as of June 29, 2024 [1].",
        1,
    )


def test_target_abstains_when_no_excerpt_answers():
    text, cited = target_answer(asked(relevant=frozenset({"c9"})), EXCERPTS)
    assert cited is None
    assert text == "The filings available on August 2, 2024 10:01 UTC do not say."
    assert abstained(text)


def test_a_relevant_excerpt_must_print_the_value():
    text, cited = target_answer(asked(relevant=frozenset({"c1"})), EXCERPTS)
    assert cited is None and abstained(text)


def test_example_carries_the_prompt_target_and_labels():
    item = example(asked(), EXCERPTS)
    assert item["as_of"] == AT.isoformat()
    assert item["messages"][0]["role"] == "system" and moment(AT) in item["messages"][0]["content"]
    assert item["messages"][1]["content"].endswith(f"Question: {asked().text}")
    assert item["excerpts"] == ["c1", "c2"] and item["relevant"] == ["c2"]
    assert item["cited"] == 2 and item["answers"] == ["85,777", "85,777,000"]
    json.dumps(item)


def test_a_third_of_training_questions_also_learn_to_abstain_without_the_answer():
    ids = [f"q{n}" for n in range(300)]
    share = sum(map(withheld, ids)) / len(ids)
    assert 0.25 < share < 0.42
    chosen = next(i for i in ids if withheld(i))
    skipped = next(i for i in ids if not withheld(i))
    ranked = [EXCERPTS[1], *(Excerpt(f"d{n}", "Apple 10-Q", "Other text.") for n in range(7))]

    plain, hidden = examples_for(asked(question_id=chosen, split="train"), ranked)
    assert plain["excerpts"] == ["c2", "d0", "d1", "d2", "d3"] and plain["cited"] == 1
    assert hidden["question_id"] == f"{chosen}:withheld" and hidden["variant"] == "withheld"
    assert hidden["excerpts"] == ["d0", "d1", "d2", "d3", "d4"] and hidden["relevant"] == []
    assert hidden["cited"] is None and abstained(hidden["target"])

    assert len(examples_for(asked(question_id=skipped, split="train"), ranked)) == 1
    assert len(examples_for(asked(question_id=chosen, split="test"), ranked)) == 1


def test_score_counts_answers_citations_and_abstentions():
    answerable = example(asked(), EXCERPTS)
    unanswerable = example(asked(question_id="q2", relevant=frozenset({"c9"})), EXCERPTS)
    found = score(
        [answerable, unanswerable],
        {"q1": "Apple reported $85,777 million [2].", "q2": "The filings do not say."},
    )
    assert found == {
        "correct": 1.0,
        "accuracy": 0.5,
        "cited_relevant": 0.5,
        "abstained_when_unanswerable": 1.0,
        "abstained_when_answerable": 0.0,
        "answerable": 0.5,
        "questions": 2.0,
    }
    wrong = score([answerable], {"q1": "It was 85,777,001 [1]. The filings do not say."})
    assert wrong["accuracy"] == 0.0 and wrong["cited_relevant"] == 0.0 and wrong["correct"] == 0.0
    assert wrong["abstained_when_answerable"] == 1.0
    with pytest.raises(ValueError):
        score([], {})


def test_scored_prefixes_each_model_on_the_questions_it_answered():
    items = [example(asked(), EXCERPTS), example(asked(question_id="q2"), EXCERPTS)]
    answers = by_model(
        [
            {"question_id": "q1", "model": "base", "answer": "85,777 [2]"},
            {"question_id": "q2", "model": "base", "answer": "no idea"},
            {"question_id": "q1", "model": "lora", "answer": "85,777 [2]"},
            {"question_id": "q2", "model": "lora", "answer": "85,777 [2]"},
        ]
    ) | {"reference": {"q1": "85,777 [1]"}}
    found = scored(items, answers)
    assert found["base_accuracy"] == 0.5 and found["lora_accuracy"] == 1.0
    assert found["reference_questions"] == 1.0 and found["reference_cited_relevant"] == 0.0


def test_jsonl_round_trip_and_digest(tmp_path):
    path = tmp_path / "rows.jsonl"
    assert write_jsonl(path, [{"b": 1, "a": 2}, {"c": 3}]) == 2
    assert read_jsonl(path) == [{"a": 2, "b": 1}, {"c": 3}]
    assert path.read_text().splitlines()[0] == '{"a": 2, "b": 1}'
    assert len(file_sha256(path)) == 64


def test_job_args():
    args = prepare_args(["--catalog", "pitlake_dev", "--run-id", "42"])
    assert (args.catalog, args.run_id, args.retrieval_run) == ("pitlake_dev", "42", "")
    args = score_args(
        [
            "--catalog",
            "pitlake_dev",
            "--experiment",
            "/x",
            "--reference-sample",
            "",
            "--run-id",
            "7",
        ]
    )
    assert (args.reference_sample, args.chat_endpoint) == (0, "")
    with pytest.raises(ValueError):
        prepare_args(["--catalog", "bad catalog"])

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from pitlake.gold import ANNUAL, INSTANT, METRICS, QUARTER
from pitlake.questions import (
    LABELS,
    TEST,
    TRAIN,
    VALIDATION,
    Fact,
    Passage,
    answer_pattern,
    answer_strings,
    build_questions,
    first_reports,
    phrase,
    split_of,
)

Q3_10Q = "0000320193-24-000081"
FY_10K = "0000320193-24-000123"
NEXT_10Q = "0000320193-25-000008"
ACCEPTED = datetime(2024, 8, 2, 10, 1, tzinfo=UTC)
TRAIN_UNTIL = datetime(2024, 9, 1, tzinfo=UTC)
TEST_FROM = datetime(2025, 1, 1, tzinfo=UTC)


def fact(metric="revenue", value="85777000000", accession=Q3_10Q, at=ACCEPTED, **kw):
    fields = {
        "symbol": "AAPL",
        "duration": QUARTER,
        "period_end": date(2024, 6, 29),
        "unit": "USD",
        **kw,
    }
    return Fact(
        fields["symbol"],
        metric,
        fields["duration"],
        fields["period_end"],
        Decimal(value),
        fields["unit"],
        accession,
        at,
    )


PASSAGES = [
    Passage("c-income", "AAPL", Q3_10Q, "Total net sales | 85,777 | 81,797\nNet income | 21,448"),
    Passage("c-mdna", "AAPL", Q3_10Q, "Net sales were $85,777 million, up 5%."),
    Passage("c-other", "AAPL", Q3_10Q, "Share repurchases | 185,777 | 2,857.77"),
    Passage("c-later", "AAPL", NEXT_10Q, "Total net sales | 85,777 (prior year)"),
]


def test_every_gold_metric_has_a_question_label():
    assert {m.name for m in METRICS} <= set(LABELS)


@pytest.mark.parametrize(
    "value, unit, expected",
    [
        ("85777000000", "USD", ("85,777", "85,777,000")),
        ("-142000000", "USD", ("142", "142,000")),
        ("1.4", "USD/shares", ("1.40",)),
        ("15204137000", "shares", ("15,204,137,000", "15,204,137")),
        ("123456789", "USD", ()),
    ],
)
def test_answer_strings(value, unit, expected):
    assert answer_strings(Decimal(value), unit) == expected


def test_answer_pattern_matches_whole_numbers_only():
    pattern = answer_pattern(["85,777"])
    assert pattern.search("Total net sales | 85,777 | 81,797")
    assert pattern.search("($85,777)")
    assert not pattern.search("185,777")
    assert not pattern.search("85,7770")
    assert not pattern.search("85,777,000")
    assert not pattern.search("85,777.5")
    assert pattern.search("was 85,777.")
    with pytest.raises(ValueError):
        answer_pattern([])


def test_split_of():
    assert split_of(datetime(2024, 1, 1, tzinfo=UTC), TRAIN_UNTIL, TEST_FROM) == TRAIN
    assert split_of(TRAIN_UNTIL, TRAIN_UNTIL, TEST_FROM) == VALIDATION
    assert split_of(TEST_FROM, TRAIN_UNTIL, TEST_FROM) == TEST


def test_first_reports_keep_the_filing_that_made_a_value_knowable():
    later = fact(accession=NEXT_10Q, at=datetime(2025, 1, 31, tzinfo=UTC), value="85000000000")
    [kept] = first_reports([later, fact()])
    assert kept.accession_number == Q3_10Q and kept.value == Decimal("85777000000")


def test_phrases_name_company_metric_and_period():
    text = phrase(fact(), TRAIN)
    assert "Apple Inc." in text and "total net sales" in text
    assert "three months ended June 29, 2024" in text
    annual = phrase(fact(duration=ANNUAL, period_end=date(2024, 9, 28)), TRAIN)
    assert "fiscal year ended September 28, 2024" in annual
    instant = phrase(fact("assets", duration=INSTANT), TEST)
    assert instant == "As of June 29, 2024, how much total assets did Apple Inc. have?"


@pytest.mark.parametrize("duration", [QUARTER, INSTANT])
def test_each_split_uses_phrasings_the_others_never_see(duration):
    facts = [
        fact(metric, period_end=date(2020 + i, 3, 31), duration=duration)
        for i in range(5)
        for metric in ("revenue", "net_income", "gross_profit", "operating_income")
    ]
    starts = {
        split: {phrase(f, split).split("Apple")[0] for f in facts}
        for split in (TRAIN, VALIDATION, TEST)
    }
    assert not starts[TRAIN] & starts[VALIDATION]
    assert not starts[TRAIN] & starts[TEST]
    assert not starts[VALIDATION] & starts[TEST]


def test_build_questions_labels_chunks_of_the_reporting_filing():
    questions, dropped = build_questions(
        [
            fact(),
            fact("net_income", value="21448000000"),
            fact("cash", value="25565000000", duration=INSTANT),
            fact("revenue", accession="0000320193-99-000001", period_end=date(2024, 3, 30)),
            fact("equity", value="66708123456", duration=INSTANT),
            fact("unlisted_metric"),
        ],
        PASSAGES,
        TRAIN_UNTIL,
        TEST_FROM,
    )
    by_metric = {q.metric: q for q in questions}
    assert set(by_metric) == {"revenue", "net_income"}
    assert by_metric["revenue"].relevant == ("c-income", "c-mdna")
    assert by_metric["net_income"].relevant == ("c-income",)
    assert by_metric["revenue"].split == TRAIN and by_metric["revenue"].as_of == ACCEPTED
    assert dropped == {
        "value_not_in_text:cash": 1,
        "no_document": 1,
        "no_answer_string": 1,
    }
    assert len({q.question_id for q in questions}) == len(questions)


def test_the_test_split_must_follow_training():
    with pytest.raises(ValueError):
        build_questions([], [], TEST_FROM, TRAIN_UNTIL)

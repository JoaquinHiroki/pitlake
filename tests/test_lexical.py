import math

import numpy as np
import pytest

from pitlake.lexical import Lexicon, fuse, tokens, top
from pitlake.reranker import candidate_metrics

DOCS = [
    "Apple 10-Q. Total net sales | 85,777 | 81,797",
    "Apple 10-Q. Services net sales were 24,213.",
    "Apple 10-Q. Risk factors and legal proceedings.",
]


def test_tokens_are_lowercase_words_and_numbers():
    assert tokens("Total net sales | 85,777") == ["total", "net", "sales", "85", "777"]


def test_bm25_matches_the_formula():
    lexicon = Lexicon.build(["a b", "a"])
    found = lexicon.scores("b", np.ones(2, dtype=bool))
    idf = math.log(1 + (2 - 1 + 0.5) / (1 + 0.5))
    norm = 1.2 * (1 - 0.75 + 0.75 * 2 / 1.5)
    assert found[0] == pytest.approx(idf * 2.2 / (1 + norm))
    assert found[1] == 0


def test_the_passage_with_the_question_words_ranks_first():
    lexicon = Lexicon.build(DOCS)
    mask = np.ones(3, dtype=bool)
    assert top(lexicon.scores("Report total net sales.", mask), mask, 3) == [0, 1, 2]


def test_statistics_come_only_from_visible_passages():
    later = "Apple 10-K. Total net sales total net sales total net sales."
    mask = np.array([True, True, True, False])
    with_later = Lexicon.build([*DOCS, later]).scores("total net sales", mask)
    without = Lexicon.build(DOCS).scores("total net sales", np.ones(3, dtype=bool))
    np.testing.assert_allclose(with_later[:3], without)
    assert with_later[3] == 0


def test_top_returns_only_visible_passages_and_breaks_ties_by_position():
    scores = np.array([1.0, 2.0, 2.0, 5.0])
    mask = np.array([True, True, True, False])
    assert top(scores, mask, 3) == [1, 2, 0]
    assert top(np.zeros(4), mask, 2) == [0, 1]


def test_fusion_rewards_agreement_and_is_deterministic():
    assert fuse([[1, 2, 3], [3, 2, 1]], 3) == [1, 3, 2]
    assert fuse([[1, 2], [2, 1]], 2) == [1, 2]
    assert fuse([[1, 2, 3], [4, 2]], 2) == [2, 1]
    assert fuse([[7], []], 5) == [7]


def test_candidate_metrics_report_the_ceiling():
    ranked = [("q1", ["a", "b", "c"], {"c"}), ("q2", ["a"] * 20 + ["z"], {"z"})]
    found = candidate_metrics(ranked)
    assert found["mrr_at_10"] == pytest.approx(1 / 3 / 2)
    assert found["hit_at_20"] == 0.5
    assert "future_chunks" not in found

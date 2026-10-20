import numpy as np
import pytest

from pitlake.retrieval import (
    Adapter,
    Corpus,
    Query,
    TrainConfig,
    evaluate,
    l2_score,
    metrics,
    normalize_rows,
    rank,
    rankings_fingerprint,
    recall,
    search,
    squared_distances,
    train_adapter,
)

T0 = 1_700_000_000_000_000


def corpus_of(vectors, knowable=None, symbols=None, prefix="c"):
    n = len(vectors)
    knowable = knowable if knowable is not None else [T0] * n
    symbols = symbols if symbols is not None else ["AAPL"] * n
    return Corpus.build(
        [
            (f"{prefix}{i:04d}", v, int(k), s)
            for i, (v, k, s) in enumerate(zip(vectors, knowable, symbols, strict=True))
        ]
    )


def test_squared_distances_are_euclidean():
    q = np.array([[0.0, 0.0], [1.0, 1.0]])
    d = np.array([[3.0, 4.0], [1.0, 1.0]])
    assert np.allclose(squared_distances(q, d), [[25.0, 2.0], [13.0, 0.0]])
    assert np.allclose(l2_score([0.0, 1.0, 3.0]), [1.0, 0.5, 0.25])


def test_corpus_is_sorted_normalized_and_unique():
    corpus = Corpus.build([("b", [3.0, 4.0], T0, "MSFT"), ("a", [0.0, 2.0], T0, "AAPL")])
    assert corpus.ids == ("a", "b")
    assert np.allclose(corpus.vectors, [[0.0, 1.0], [0.6, 0.8]])
    with pytest.raises(ValueError):
        Corpus.build([("a", [1.0, 0.0], T0, "X"), ("a", [0.0, 1.0], T0, "X")])
    with pytest.raises(ValueError):
        Corpus.build([("a", [0.0, 0.0], T0, "X")])
    with pytest.raises(ValueError):
        Corpus.build([("a", [float("nan"), 1.0], T0, "X")])


def test_search_never_returns_what_was_not_knowable():
    vectors = [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]]
    corpus = corpus_of(vectors, knowable=[T0 + 10, T0, T0])
    query = Query("q", np.array([1.0, 0.0]), T0 + 9, "AAPL", frozenset({"c0000"}))
    [ranking] = rank(corpus, [query])
    assert [corpus.ids[h.index] for h in ranking.hits] == ["c0001", "c0002"]
    assert ranking.future == 0 and ranking.first_relevant is None
    [unfiltered] = rank(corpus, [query], point_in_time=False)
    assert corpus.ids[unfiltered.hits[0].index] == "c0000"
    assert unfiltered.future == 1 and unfiltered.first_relevant == 1
    [at_acceptance] = rank(corpus, [Query("q", query.vector, T0 + 10, "AAPL", query.relevant)])
    assert at_acceptance.first_relevant == 1


def test_search_filters_on_symbol_and_breaks_ties_by_id():
    corpus = corpus_of([[1.0, 0.0]] * 3, symbols=["MSFT", "AAPL", "AAPL"])
    hits = search(corpus, np.array([[1.0, 0.0]]), [corpus.visible(T0, "AAPL")], k=5)[0]
    assert [h.index for h in hits] == [1, 2]
    assert search(corpus, np.array([[1.0, 0.0]]), [corpus.visible(T0 - 1)], k=5) == [[]]


def test_search_keeps_the_k_nearest_in_order():
    rng = np.random.default_rng(0)
    vectors = normalize_rows(rng.normal(size=(300, 8)))
    corpus = corpus_of(vectors)
    query = rng.normal(size=(1, 8))
    hits = search(corpus, query, [corpus.visible(None)], k=10)[0]
    brute = np.argsort(squared_distances(query, corpus.vectors)[0], kind="stable")[:10]
    assert [h.index for h in hits] == brute.tolist()
    assert all(
        a.squared_distance <= b.squared_distance for a, b in zip(hits, hits[1:], strict=False)
    )


def test_metrics():
    corpus = corpus_of([[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]])
    queries = [
        Query("a", np.array([1.0, 0.0]), T0, None, frozenset({"c0000"})),
        Query("b", np.array([1.0, 0.0]), T0, None, frozenset({"c0001"})),
        Query("c", np.array([1.0, 0.0]), T0, None, frozenset({"missing"})),
    ]
    result = evaluate(corpus, queries)
    assert result["hit_at_1"] == pytest.approx(1 / 3)
    assert result["hit_at_5"] == pytest.approx(2 / 3)
    assert result["mrr_at_10"] == pytest.approx((1 + 1 / 3) / 3)
    assert result["questions"] == 3 and result["future_chunks"] == 0
    with pytest.raises(ValueError):
        metrics([])


def test_adapter_starts_as_the_base_model_and_round_trips():
    adapter = Adapter.init(8, 2, seed=1)
    q = np.random.default_rng(1).normal(size=(3, 8))
    assert np.array_equal(adapter.apply(q), q)
    adapter.b += 0.5
    again = Adapter.from_bytes(adapter.to_bytes())
    assert again.fingerprint == adapter.fingerprint and np.array_equal(
        again.apply(q), adapter.apply(q)
    )
    assert Adapter.init(8, 2, seed=2).fingerprint != Adapter.init(8, 2, seed=1).fingerprint
    with pytest.raises(ValueError):
        Adapter.init(8, 8, seed=1)


def distorted_task(seed=3, docs=240, dim=24):
    rng = np.random.default_rng(seed)
    vectors = normalize_rows(rng.normal(size=(docs, dim)))
    u = normalize_rows(rng.normal(size=(1, dim)))[0]
    v = normalize_rows(rng.normal(size=(1, dim)))[0]
    corpus = corpus_of(vectors, knowable=[T0 + i for i in range(docs)])

    def question(n, j):
        d = corpus.vectors[j]
        q = d + 12.0 * (d @ v) * u + 0.05 * rng.normal(size=dim)
        return Query(f"q{n:04d}", q, T0 + docs, None, frozenset({corpus.ids[j]}))

    picks = rng.permutation(docs)
    queries = [question(n, int(j)) for n, j in enumerate(picks)]
    return corpus, queries[:140], queries[140:180], queries[180:]


def test_the_adapter_learns_a_low_rank_distortion_and_generalizes():
    corpus, train, validation, test = distorted_task()
    base = evaluate(corpus, test)
    config = TrainConfig(rank=2, temperature=0.1, learning_rate=0.02, epochs=60, refresh_every=5)
    seen = []
    adapter, history = train_adapter(corpus, train, validation, config, on_epoch=seen.append)
    tuned = evaluate(corpus, test, adapter)
    assert tuned["mrr_at_10"] > base["mrr_at_10"] + 0.2
    assert tuned["hit_at_1"] > base["hit_at_1"]
    assert history[0]["epoch"] == 0 and seen == history[1:]
    assert history[-1]["loss"] < history[1]["loss"]


def test_training_is_deterministic():
    corpus, train, validation, _ = distorted_task()
    config = TrainConfig(rank=2, temperature=0.1, learning_rate=0.02, epochs=10, refresh_every=5)
    first, _ = train_adapter(corpus, train, validation, config)
    second, _ = train_adapter(corpus, train, validation, config)
    assert first.fingerprint == second.fingerprint


def test_training_needs_labelled_questions_in_both_splits():
    corpus, train, validation, _ = distorted_task()
    with pytest.raises(ValueError):
        train_adapter(corpus, train, [], TrainConfig())
    unlabelled = [Query("x", train[0].vector, T0, None, frozenset())]
    with pytest.raises(ValueError):
        train_adapter(corpus, unlabelled, validation, TrainConfig())


def test_rankings_fingerprint_and_recall():
    corpus, _, _, test = distorted_task()
    rankings = rank(corpus, test)
    assert rankings_fingerprint(corpus, rankings) == rankings_fingerprint(
        corpus, list(reversed(rank(corpus, test)))
    )
    assert rankings_fingerprint(corpus, rankings) != rankings_fingerprint(corpus, rankings[1:])
    assert recall(["a", "b"], ["b", "c"]) == 0.5 and recall([], ["x"]) == 1.0


@pytest.mark.parametrize(
    "bad", [{"temperature": 0}, {"learning_rate": -1}, {"epochs": 0}, {"patience": 0}]
)
def test_train_config_validation(bad):
    with pytest.raises(ValueError):
        TrainConfig(**bad)

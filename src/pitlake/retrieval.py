import hashlib
import io
import json
from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np

K_VALUES = (1, 5, 10)
TOP_K = max(K_VALUES)
DECIMALS = 9


def as_matrix(vectors) -> np.ndarray:
    matrix = np.asarray(vectors, dtype=np.float64)
    if matrix.ndim != 2 or not matrix.size:
        raise ValueError(f"expected a non-empty 2-D array of vectors, got shape {matrix.shape}")
    if not np.isfinite(matrix).all():
        raise ValueError("vectors contain NaN or infinity")
    return matrix


def normalize_rows(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if (norms == 0).any():
        raise ValueError("cannot normalize a zero vector")
    return matrix / norms


def squared_distances(queries: np.ndarray, docs: np.ndarray) -> np.ndarray:
    q2 = np.einsum("ij,ij->i", queries, queries)[:, None]
    d2 = np.einsum("ij,ij->i", docs, docs)[None, :]
    return np.maximum(q2 + d2 - 2.0 * queries @ docs.T, 0.0)


def l2_score(squared_distance):
    return 1.0 / (1.0 + np.asarray(squared_distance, dtype=np.float64))


@dataclass(frozen=True)
class Corpus:
    ids: tuple[str, ...]
    vectors: np.ndarray
    knowable_us: np.ndarray
    symbols: np.ndarray

    @classmethod
    def build(cls, rows: Sequence[tuple[str, Sequence[float], int, str]]) -> "Corpus":
        ordered = sorted(rows, key=lambda r: r[0])
        ids = tuple(r[0] for r in ordered)
        if len(set(ids)) != len(ids):
            raise ValueError("chunk ids must be unique")
        return cls(
            ids=ids,
            vectors=normalize_rows(as_matrix([r[1] for r in ordered])),
            knowable_us=np.array([r[2] for r in ordered], dtype=np.int64),
            symbols=np.array([r[3] for r in ordered], dtype=object),
        )

    def __len__(self) -> int:
        return len(self.ids)

    def index(self) -> dict[str, int]:
        return {c: i for i, c in enumerate(self.ids)}

    def visible(self, as_of_us: int | None, symbol: str | None = None) -> np.ndarray:
        mask = np.ones(len(self), dtype=bool)
        if as_of_us is not None:
            mask &= self.knowable_us <= as_of_us
        if symbol is not None:
            mask &= self.symbols == symbol
        return mask


@dataclass(frozen=True)
class Query:
    id: str
    vector: np.ndarray
    as_of_us: int
    symbol: str | None
    relevant: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class Hit:
    index: int
    squared_distance: float


def search(
    corpus: Corpus, vectors: np.ndarray, masks: Sequence[np.ndarray], k: int = TOP_K
) -> list[list[Hit]]:
    if len(vectors) != len(masks):
        raise ValueError("one mask per query")
    distances = squared_distances(vectors, corpus.vectors)
    order = np.arange(len(corpus))
    results = []
    for row, mask in zip(distances, masks, strict=True):
        candidates = np.flatnonzero(mask)
        if not len(candidates):
            results.append([])
            continue
        values = row[candidates]
        if len(candidates) > k:
            keep = np.argpartition(values, k - 1)[:k]
            bound = values[keep].max()
            keep = np.flatnonzero(values <= bound)
            candidates, values = candidates[keep], values[keep]
        ranked = np.lexsort((order[candidates], values))[:k]
        results.append([Hit(int(candidates[i]), float(values[i])) for i in ranked])
    return results


@dataclass
class Adapter:
    a: np.ndarray
    b: np.ndarray

    @classmethod
    def init(cls, dimension: int, rank: int, seed: int) -> "Adapter":
        if not 0 < rank < dimension:
            raise ValueError(f"rank must be between 1 and {dimension - 1}")
        rng = np.random.default_rng(seed)
        a = rng.normal(0.0, 1.0 / np.sqrt(dimension), size=(dimension, rank))
        return cls(a, np.zeros((rank, dimension)))

    @property
    def rank(self) -> int:
        return self.a.shape[1]

    def apply(self, queries: np.ndarray) -> np.ndarray:
        return queries + (queries @ self.a) @ self.b

    def copy(self) -> "Adapter":
        return Adapter(self.a.copy(), self.b.copy())

    def to_bytes(self) -> bytes:
        buffer = io.BytesIO()
        np.savez(buffer, a=self.a, b=self.b)
        return buffer.getvalue()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Adapter":
        with np.load(io.BytesIO(data)) as saved:
            return cls(saved["a"].astype(np.float64), saved["b"].astype(np.float64))

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        for matrix in (self.a, self.b):
            digest.update(json.dumps(matrix.shape).encode())
            digest.update(np.ascontiguousarray(matrix, dtype="<f8").tobytes())
        return digest.hexdigest()


@dataclass(frozen=True)
class TrainConfig:
    rank: int = 16
    temperature: float = 0.05
    learning_rate: float = 1e-4
    epochs: int = 150
    batch_size: int = 64
    hard_negatives: int = 32
    random_negatives: int = 32
    refresh_every: int = 10
    patience: int = 5
    seed: int = 7

    def __post_init__(self) -> None:
        if self.temperature <= 0 or self.learning_rate <= 0:
            raise ValueError("temperature and learning_rate must be positive")
        if min(self.epochs, self.batch_size, self.refresh_every, self.patience) < 1:
            raise ValueError("epochs, batch_size, refresh_every and patience must be positive")

    def to_dict(self) -> dict:
        return dict(vars(self))


def relevant_indices(corpus: Corpus, queries: Sequence[Query]) -> list[np.ndarray]:
    index = corpus.index()
    return [np.array(sorted(index[c] for c in q.relevant if c in index)) for q in queries]


def masks_for(corpus: Corpus, queries: Sequence[Query], point_in_time: bool = True):
    return [corpus.visible(q.as_of_us if point_in_time else None, q.symbol) for q in queries]


def hard_negatives(corpus, vectors, masks, positives, count: int) -> list[np.ndarray]:
    hits = search(corpus, vectors, masks, count + max((len(p) for p in positives), default=0))
    return [
        np.array([h.index for h in found if h.index not in set(pos.tolist())][:count], dtype=int)
        for found, pos in zip(hits, positives, strict=True)
    ]


def _gradient(corpus, adapted, positives, negatives, temperature) -> tuple[np.ndarray, float]:
    gradients = np.zeros_like(adapted)
    loss = 0.0
    for i, (pos, neg) in enumerate(zip(positives, negatives, strict=True)):
        chosen = np.concatenate([pos, neg]).astype(int)
        docs = corpus.vectors[chosen]
        logits = -((adapted[i] - docs) ** 2).sum(axis=1) / temperature
        logits -= logits.max()
        p = np.exp(logits)
        p /= p.sum()
        w = np.zeros_like(p)
        w[: len(pos)] = p[: len(pos)] / p[: len(pos)].sum()
        loss += -np.log(p[: len(pos)].sum())
        gradients[i] = (2.0 / temperature) * ((p - w) @ docs)
    return gradients, loss


def train_adapter(
    corpus: Corpus,
    train: Sequence[Query],
    validation: Sequence[Query],
    config: TrainConfig,
    on_epoch=None,
) -> tuple[Adapter, list[dict]]:
    if not train or not validation:
        raise ValueError("training needs questions in both the training and validation splits")
    rng = np.random.default_rng(config.seed)
    base = as_matrix([q.vector for q in train])
    masks = masks_for(corpus, train)
    positives = relevant_indices(corpus, train)
    if any(not len(p) for p in positives):
        raise ValueError("every training question needs at least one relevant chunk")
    adapter = Adapter.init(base.shape[1], config.rank, config.seed)
    moments = [np.zeros_like(adapter.a), np.zeros_like(adapter.b)]
    squares = [np.zeros_like(adapter.a), np.zeros_like(adapter.b)]
    beta1, beta2, eps, step = 0.9, 0.999, 1e-8, 0

    best = adapter.copy()
    best_score = evaluate(corpus, validation, adapter)["mrr_at_10"]
    history = [{"epoch": 0, "loss": float("nan"), "validation_mrr_at_10": best_score}]
    stale = 0
    hard: list[np.ndarray] = []
    for epoch in range(1, config.epochs + 1):
        if (epoch - 1) % config.refresh_every == 0:
            hard = hard_negatives(
                corpus, adapter.apply(base), masks, positives, config.hard_negatives
            )
        order = rng.permutation(len(train))
        total = 0.0
        for start in range(0, len(order), config.batch_size):
            batch = order[start : start + config.batch_size]
            negatives = []
            for i in batch:
                pool = np.setdiff1d(np.flatnonzero(masks[i]), positives[i], assume_unique=True)
                sampled = rng.choice(pool, min(config.random_negatives, len(pool)), replace=False)
                negatives.append(np.union1d(hard[i], sampled))
            queries = base[batch]
            g, loss = _gradient(
                corpus,
                adapter.apply(queries),
                [positives[i] for i in batch],
                negatives,
                config.temperature,
            )
            total += loss
            grads = [
                queries.T @ (g @ adapter.b.T) / len(batch),
                (queries @ adapter.a).T @ g / len(batch),
            ]
            step += 1
            for j, (param, grad) in enumerate(zip((adapter.a, adapter.b), grads, strict=True)):
                moments[j] = beta1 * moments[j] + (1 - beta1) * grad
                squares[j] = beta2 * squares[j] + (1 - beta2) * grad**2
                m_hat = moments[j] / (1 - beta1**step)
                v_hat = squares[j] / (1 - beta2**step)
                param -= config.learning_rate * m_hat / (np.sqrt(v_hat) + eps)
        score = evaluate(corpus, validation, adapter)["mrr_at_10"]
        record = {"epoch": epoch, "loss": total / len(train), "validation_mrr_at_10": score}
        if score > best_score:
            best, best_score, stale = adapter.copy(), score, 0
        else:
            stale += 1
        history.append(record)
        if on_epoch is not None:
            on_epoch(record)
        if stale >= config.patience:
            break
    return best, history


@dataclass(frozen=True)
class Ranking:
    query_id: str
    hits: tuple[Hit, ...]
    first_relevant: int | None
    future: int


def rank(
    corpus: Corpus,
    queries: Sequence[Query],
    adapter: Adapter | None = None,
    point_in_time: bool = True,
    k: int = TOP_K,
) -> list[Ranking]:
    if not queries:
        return []
    vectors = as_matrix([q.vector for q in queries])
    if adapter is not None:
        vectors = adapter.apply(vectors)
    found = search(corpus, vectors, masks_for(corpus, queries, point_in_time), k)
    rankings = []
    for query, hits in zip(queries, found, strict=True):
        position = next(
            (n for n, h in enumerate(hits, 1) if corpus.ids[h.index] in query.relevant), None
        )
        future = sum(int(corpus.knowable_us[h.index] > query.as_of_us) for h in hits)
        rankings.append(Ranking(query.id, tuple(hits), position, future))
    return rankings


def metrics(rankings: Sequence[Ranking]) -> dict[str, float]:
    n = len(rankings)
    if not n:
        raise ValueError("no questions to score")
    result = {
        f"hit_at_{k}": sum(r.first_relevant is not None and r.first_relevant <= k for r in rankings)
        / n
        for k in K_VALUES
    }
    result[f"mrr_at_{TOP_K}"] = (
        sum(1.0 / r.first_relevant for r in rankings if r.first_relevant) / n
    )
    result["future_chunks"] = float(sum(r.future for r in rankings))
    result["questions"] = float(n)
    return result


def evaluate(corpus, queries, adapter=None, point_in_time: bool = True) -> dict[str, float]:
    return metrics(rank(corpus, queries, adapter, point_in_time))


def rankings_fingerprint(corpus: Corpus, rankings: Sequence[Ranking]) -> str:
    rows = [
        [r.query_id, [[corpus.ids[h.index], round(h.squared_distance, DECIMALS)] for h in r.hits]]
        for r in sorted(rankings, key=lambda r: r.query_id)
    ]
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def recall(exact: Sequence[str], approximate: Sequence[str]) -> float:
    if not exact:
        return 1.0
    return len(set(exact) & set(approximate)) / len(exact)

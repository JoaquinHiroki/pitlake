import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

_TOKEN = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75
RRF_K = 60


def tokens(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


@dataclass(frozen=True)
class Lexicon:
    postings: dict[str, tuple[np.ndarray, np.ndarray]]
    lengths: np.ndarray

    @classmethod
    def build(cls, texts: Sequence[str]) -> "Lexicon":
        found: dict[str, tuple[list[int], list[int]]] = {}
        lengths = []
        for doc, text in enumerate(texts):
            counts = Counter(tokens(text))
            lengths.append(sum(counts.values()))
            for term, count in counts.items():
                docs, freqs = found.setdefault(term, ([], []))
                docs.append(doc)
                freqs.append(count)
        postings = {
            t: (np.array(d, dtype=np.int64), np.array(f, dtype=np.float64))
            for t, (d, f) in found.items()
        }
        return cls(postings, np.array(lengths, dtype=np.float64))

    def __len__(self) -> int:
        return len(self.lengths)

    def scores(self, query: str, mask: np.ndarray) -> np.ndarray:
        scores = np.zeros(len(self))
        visible = int(mask.sum())
        if not visible:
            return scores
        average = self.lengths[mask].mean() or 1.0
        norm = K1 * (1 - B + B * self.lengths / average)
        for term in set(tokens(query)):
            if term not in self.postings:
                continue
            docs, freqs = self.postings[term]
            seen = mask[docs]
            df = int(seen.sum())
            if not df:
                continue
            idf = np.log(1 + (visible - df + 0.5) / (df + 0.5))
            docs, freqs = docs[seen], freqs[seen]
            scores[docs] += idf * freqs * (K1 + 1) / (freqs + norm[docs])
        return scores


def top(scores: np.ndarray, mask: np.ndarray, k: int) -> list[int]:
    candidates = np.flatnonzero(mask)
    order = np.lexsort((candidates, -scores[candidates]))
    return [int(i) for i in candidates[order][:k]]


def fuse(rankings: Sequence[Sequence[int]], k: int, rrf_k: int = RRF_K) -> list[int]:
    found: dict[int, float] = {}
    first: dict[int, tuple[int, int]] = {}
    for source, ranked in enumerate(rankings):
        for position, doc in enumerate(ranked, start=1):
            found[doc] = found.get(doc, 0.0) + 1.0 / (rrf_k + position)
            first.setdefault(doc, (position, source))
    return sorted(found, key=lambda d: (-found[d], first[d]))[:k]

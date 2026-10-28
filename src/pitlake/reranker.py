import random
from collections.abc import Collection, Mapping, Sequence
from datetime import datetime
from pathlib import Path

from pitlake.generator import Asked
from pitlake.rag import Excerpt, micros, moment
from pitlake.retrieval import TOP_K, Ranking, metrics

RERANK_MODEL = "system.ai.llama_v3_2_1b_instruct"
RERANKER = "reranker"
TUNED_ORDER = "tuned"
CANDIDATES = 20
POOL = 100
DENSE = "dense"
LEXICAL = "bm25"
HYBRID = "hybrid"
GROUP = 8
RERANKINGS = "rerankings.tsv"
YES = "yes"
NO = "no"
JUDGE_PROMPT = (
    "You judge whether a passage from a company's SEC filing states the figure a question asks "
    "for. The question is asked on {now}. Reply with one word: yes or no."
)


def candidate_row(asked: Asked, chunks: Sequence[tuple[str, str, str, int]]) -> dict:
    return {
        "question_id": asked.question_id,
        "split": asked.split,
        "symbol": asked.symbol,
        "as_of": asked.as_of.isoformat(),
        "as_of_us": micros(asked.as_of),
        "text": asked.text,
        "answers": list(asked.answers),
        "relevant": sorted(asked.relevant),
        "candidates": [
            {
                "chunk_id": c,
                "context": context,
                "text": text,
                "knowable_at_us": int(knowable),
                "relevant": c in asked.relevant,
            }
            for c, context, text, knowable in chunks
        ],
    }


def judge_messages(question: str, at: datetime, context: str, text: str) -> list[dict]:
    return [
        {"role": "system", "content": JUDGE_PROMPT.format(now=moment(at))},
        {
            "role": "user",
            "content": f"Passage:\n{context}\n{text}\n\nQuestion: {question}\n\n"
            "Does the passage state the figure the question asks for?",
        },
    ]


def answerable(row: dict) -> bool:
    return any(c["relevant"] for c in row["candidates"])


def training_group(row: dict, rng: random.Random, size: int = GROUP) -> list[int]:
    positives = [i for i, c in enumerate(row["candidates"]) if c["relevant"]]
    negatives = [i for i, c in enumerate(row["candidates"]) if not c["relevant"]]
    if not positives or not negatives:
        return []
    return [rng.choice(positives), *rng.sample(negatives, min(size - 1, len(negatives)))]


def reorder(ids: Sequence[str], scores: Sequence[float]) -> list[str]:
    if len(ids) != len(scores):
        raise ValueError("one score per candidate")
    order = sorted(range(len(ids)), key=lambda i: (-scores[i], i))
    return [ids[i] for i in order]


def leaked(row: dict) -> int:
    return sum(c["knowable_at_us"] > row["as_of_us"] for c in row["candidates"])


def ordering(row: dict, ranked: Sequence[str], k: int = TOP_K) -> Ranking:
    candidates = {c["chunk_id"]: c for c in row["candidates"]}
    if sorted(ranked) != sorted(candidates):
        raise ValueError(f"{row['question_id']}: the ranking is not of its candidates")
    top = ranked[:k]
    first = next((n for n, c in enumerate(top, 1) if candidates[c]["relevant"]), None)
    future = sum(candidates[c]["knowable_at_us"] > row["as_of_us"] for c in top)
    return Ranking(row["question_id"], (), first, future)


def rerank_metrics(rows: Sequence[dict], orders: Mapping[str, Sequence[str]]) -> dict[str, float]:
    found = metrics([ordering(r, orders[r["question_id"]]) for r in rows])
    found[f"hit_at_{CANDIDATES}"] = sum(answerable(r) for r in rows) / len(rows)
    return found


def candidate_metrics(ranked: Sequence[tuple[str, Sequence[str], Collection[str]]]) -> dict:
    found = metrics(
        [
            Ranking(q, (), next((n for n, c in enumerate(ids[:TOP_K], 1) if c in rel), None), 0)
            for q, ids, rel in ranked
        ]
    )
    del found["future_chunks"]
    found[f"hit_at_{CANDIDATES}"] = sum(
        any(c in rel for c in ids[:CANDIDATES]) for _, ids, rel in ranked
    ) / len(ranked)
    return found


def tuned_orders(rows: Sequence[dict]) -> dict[str, list[str]]:
    return {r["question_id"]: [c["chunk_id"] for c in r["candidates"]] for r in rows}


def top_excerpts(row: dict, ranked: Sequence[str], count: int) -> list[Excerpt]:
    candidates = {c["chunk_id"]: c for c in row["candidates"]}
    return [Excerpt(c, candidates[c]["context"], candidates[c]["text"]) for c in ranked[:count]]


def reranker_folder(staging: str, run_id: str) -> Path:
    return Path(staging) / RERANKER / run_id


def write_rerankings(path: Path, rows: Sequence[tuple[str, str, Sequence[str]]]) -> None:
    lines = sorted(f"{q}\t{model}\t{','.join(ranked)}\n" for q, model, ranked in rows)
    path.write_text("".join(lines))


def read_rerankings(path: Path) -> dict[str, dict[str, list[str]]]:
    found: dict[str, dict[str, list[str]]] = {}
    for line in path.read_text().splitlines():
        question, model, ranked = line.split("\t")
        found.setdefault(model, {})[question] = ranked.split(",")
    return found

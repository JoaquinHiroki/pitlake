import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pitlake.documents import company, long_date
from pitlake.gold import INSTANT
from pitlake.questions import LABELS, PERIODS, answer_pattern
from pitlake.rag import Excerpt, answered, citations, moment, prompt_messages

BASE_MODEL = "system.ai.llama_v3_2_3b_instruct"
BASE = "base"
LORA = "lora"
REFERENCE = "reference"
GENERATOR = "generator"
MANIFEST = "manifest.json"
PREDICTIONS = "predictions.jsonl"
TRAINING = "training.json"
ADAPTER_DIR = "adapter"
ABSTAIN = "The filings available on {now} do not say."
_SCALES = ((Decimal(1), ""), (Decimal(1_000_000), " million"), (Decimal(1_000), " thousand"))


@dataclass(frozen=True)
class Asked:
    question_id: str
    split: str
    symbol: str
    metric: str
    duration: str
    period_end: date
    value: Decimal
    as_of: datetime
    text: str
    answers: tuple[str, ...]
    relevant: frozenset[str]


def amount(value: Decimal, metric: str, printed: str) -> str:
    magnitude = abs(Decimal(value))
    sign = "-" if value < 0 else ""
    if metric == "eps_diluted":
        return f"{sign}${printed}"
    scale = next((s for d, s in _SCALES if f"{magnitude / d:,.0f}" == printed), "")
    money = "" if metric == "shares_outstanding" else "$"
    return f"{sign}{money}{printed}{scale}"


def target_answer(asked: Asked, excerpts: Sequence[Excerpt]) -> tuple[str, int | None]:
    for n, excerpt in enumerate(excerpts, start=1):
        if excerpt.chunk_id not in asked.relevant:
            continue
        printed = next((a for a in asked.answers if answer_pattern([a]).search(excerpt.text)), None)
        if printed is None:
            continue
        when = long_date(asked.period_end)
        if asked.duration == INSTANT:
            period = f"as of {when}"
        else:
            period = f"for the {PERIODS[asked.duration].format(date=when)}"
        figure = amount(asked.value, asked.metric, printed)
        return (
            f"{company(asked.symbol)} reported {LABELS[asked.metric]} of {figure} {period} [{n}].",
            n,
        )
    return ABSTAIN.format(now=moment(asked.as_of)), None


def example(asked: Asked, excerpts: Sequence[Excerpt]) -> dict:
    target, cited = target_answer(asked, excerpts)
    return {
        "question_id": asked.question_id,
        "split": asked.split,
        "as_of": asked.as_of.isoformat(),
        "messages": prompt_messages(asked.text, asked.as_of, excerpts),
        "target": target,
        "cited": cited,
        "answers": list(asked.answers),
        "excerpts": [e.chunk_id for e in excerpts],
        "relevant": [e.chunk_id for e in excerpts if e.chunk_id in asked.relevant],
    }


def abstained(answer: str) -> bool:
    return "do not say" in answer.lower()


def score(examples: Sequence[dict], answers: dict[str, str]) -> dict[str, float]:
    if not examples:
        raise ValueError("no examples to score")
    correct = cited = abstain_right = abstain_wrong = answerable = 0
    for item in examples:
        answer = answers[item["question_id"]]
        relevant = set(item["relevant"])
        used = citations(answer, len(item["excerpts"]))
        correct += answered(answer, item["answers"])
        cited += any(item["excerpts"][n - 1] in relevant for n in used)
        if relevant:
            answerable += 1
            abstain_wrong += abstained(answer)
        else:
            abstain_right += abstained(answer)
    total = len(examples)
    unanswerable = total - answerable
    return {
        "accuracy": correct / total,
        "cited_relevant": cited / total,
        "abstained_when_unanswerable": abstain_right / unanswerable if unanswerable else 1.0,
        "abstained_when_answerable": abstain_wrong / answerable if answerable else 0.0,
        "answerable": answerable / total,
        "questions": float(total),
    }


def generator_folder(staging: str, run_id: str) -> Path:
    return Path(staging) / GENERATOR / run_id


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    count = 0
    with path.open("w") as out:
        for row in rows:
            out.write(json.dumps(row, sort_keys=True) + "\n")
            count += 1
    return count


def read_jsonl(path: Path) -> list[dict]:
    with path.open() as found:
        return [json.loads(line) for line in found if line.strip()]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as found:
        for block in iter(lambda: found.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()

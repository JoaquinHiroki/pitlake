import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from random import Random
from statistics import median

from pitlake.backtest import as_utc, fingerprint
from pitlake.harness import (
    AGENT_DECIDER,
    MAX_TOKENS,
    SYMBOLS,
    TEMPERATURE,
    Complete,
    Sample,
    View,
    agent_config,
    canonical,
    parse_decision,
)
from pitlake.monotonicity import CLOSE

BLIND = "blind"
RECALL = "recall"
TOOLED = "tooled"
CONDITIONS = (BLIND, RECALL, TOOLED)
BEFORE = "before"
AFTER = "after"
ACROSS = "across"
STRADDLES = "straddles"
PRIOR = "prior"
RECALL_GAIN = "recall_gain"
TOOL_GAIN = "tool_gain"
APPARENT = "apparent"
PARTS = (PRIOR, RECALL_GAIN, TOOL_GAIN, APPARENT)
MEMORISED = "memorised"
INFERRED = "inferred"
TOOLS_OVER_MEMORY = "tools_over_memory"
CONTRASTS = ((MEMORISED, ACROSS), (INFERRED, AFTER), (TOOLS_OVER_MEMORY, ACROSS))
ROUNDS = 10000
LEVEL = 0.95
PROBES = 3
TOLERANCE = 0.05

UNTOOLED_SYSTEM = {
    BLIND: (
        "You are a research analyst. You have no tools and no data: answer from what you know. "
        "Rate each of {symbols} as overweight, neutral or underweight against an equal-weight "
        "portfolio of the five for the coming month."
    ),
    RECALL: (
        "You are a research analyst working at the instant {instant} UTC. For you it is now "
        "{instant} UTC: you know nothing published after it. You have no tools and no data: "
        "answer from what you knew at the instant, and do not use your own knowledge of markets "
        "after it. Rate each of {symbols} as overweight, neutral or underweight against an "
        "equal-weight portfolio of the five for the month after the instant."
    ),
}
UNTOOLED_TASK = (
    'Rate each symbol. Reply with only a JSON object that maps each of {symbols} to "overweight", '
    '"neutral" or "underweight", and nothing else.'
)
PROBE_SYSTEM = "Answer from memory. Every date you are asked about is in the past."
PROBE_QUESTION = (
    "What was the closing price of {name} on {day}? Give your best estimate as a single number, "
    "even if unsure, and nothing else."
)
NAMES = {
    "BTCUSDT": "Bitcoin (BTC) in US dollars",
    "SPY": "the SPDR S&P 500 ETF (SPY)",
    "AAPL": "Apple (AAPL) stock",
    "MSFT": "Microsoft (MSFT) stock",
    "NVDA": "NVIDIA (NVDA) stock",
}
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")


def text_of(content) -> str:
    if isinstance(content, list):
        return "".join(p.get("text") or "" for p in content if p.get("type") == "text")
    return content or ""


def untooled_prompts(condition: str, instant: datetime) -> list[dict]:
    names = ", ".join(SYMBOLS)
    when = f"{as_utc(instant):%Y-%m-%d %H:%M}"
    return [
        {
            "role": "system",
            "content": UNTOOLED_SYSTEM[condition].format(instant=when, symbols=names),
        },
        {"role": "user", "content": UNTOOLED_TASK.format(symbols=names)},
    ]


def condition_config(condition: str, endpoint: str) -> dict:
    if condition == TOOLED:
        return agent_config(endpoint)
    return {
        "decider": AGENT_DECIDER,
        "condition": condition,
        "endpoint": endpoint,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "system": UNTOOLED_SYSTEM[condition],
        "task": UNTOOLED_TASK,
        "symbols": list(SYMBOLS),
    }


def run_untooled(complete: Complete, condition: str, instant: datetime) -> Sample:
    reply = complete(untooled_prompts(condition, instant), None)
    text = text_of(reply.get("content"))
    try:
        decision, error = canonical(parse_decision(text)), None
    except ValueError as found:
        decision, error = None, str(found)
    return Sample(decision, fingerprint([]), (), 1, error, text[:2000])


def cutoff_instant(day: date) -> datetime:
    return datetime.combine(day, time(0), UTC)


def era(at: datetime, days: int, cutoff: date) -> str:
    start, edge = as_utc(at), cutoff_instant(cutoff)
    if start + timedelta(days=days) <= edge:
        return BEFORE
    return AFTER if start >= edge else STRADDLES


def probe_prompts(symbol: str, day: date) -> list[dict]:
    return [
        {"role": "system", "content": PROBE_SYSTEM},
        {"role": "user", "content": PROBE_QUESTION.format(name=NAMES[symbol], day=day.isoformat())},
    ]


def parse_price(text: str | None) -> float | None:
    found = NUMBER.search(DATE.sub(" ", text or ""))
    if not found:
        return None
    value = float(found.group(0).replace(",", ""))
    return value if value > 0 else None


def estimate(answers: Iterable[str | None]) -> float | None:
    values = [v for v in (parse_price(a) for a in answers) if v is not None]
    return median(values) if values else None


def recall_error(found: float | None, actual: float) -> float | None:
    if found is None or not actual:
        return None
    return abs(math.log(found / actual))


@dataclass(frozen=True)
class Question:
    symbol: str
    session: date
    actual: float

    @property
    def key(self) -> tuple[str, date]:
        return self.symbol, self.session


def questions(closes: Iterable[tuple], times: Iterable[datetime]) -> dict[datetime, list[Question]]:
    rows = [r for r in closes if r[1] == CLOSE and r[0] in SYMBOLS]
    traded = [s for s in SYMBOLS if any(r[0] == s for r in rows)]
    found = {}
    for at in sorted({as_utc(t) for t in times}):
        view, asked = View(rows, at), []
        for symbol in traded:
            last = view.latest(symbol, CLOSE)
            if last is not None and last[4]:
                asked.append(Question(symbol, as_utc(last[2]).date(), float(last[4])))
        found[at] = asked
    return found


@dataclass(frozen=True)
class Recalled:
    at: datetime
    era: str
    estimates: dict[str, float | None]
    actuals: dict[str, float]
    error: float

    def recalled(self, tolerance: float) -> bool:
        return self.error <= tolerance


def recall(
    asked: dict[datetime, list[Question]],
    estimates: dict[tuple[str, date], float | None],
    cutoff: date,
) -> list[Recalled]:
    edge, found = cutoff_instant(cutoff), []
    for at, mine in sorted(asked.items()):
        if not mine:
            continue
        errors = [recall_error(estimates.get(q.key), q.actual) for q in mine]
        found.append(
            Recalled(
                at,
                BEFORE if at <= edge else AFTER,
                {q.symbol: estimates.get(q.key) for q in mine},
                {q.symbol: q.actual for q in mine},
                median(math.inf if e is None else e for e in errors),
            )
        )
    return found


@dataclass(frozen=True)
class Scored:
    at: datetime
    era: str
    blind: float
    recall: float
    tooled: float

    def part(self, name: str) -> float:
        return {
            PRIOR: self.blind,
            RECALL_GAIN: self.recall - self.blind,
            TOOL_GAIN: self.tooled - self.recall,
            APPARENT: self.tooled,
        }[name]


def mean_of(found: Sequence[Scored], name: str) -> float | None:
    return sum(s.part(name) for s in found) / len(found) if found else None


def difference(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else a - b


def statistics(before: Sequence[Scored], after: Sequence[Scored]) -> dict[tuple[str, str], float]:
    found = {}
    for name in PARTS:
        found[name, BEFORE] = mean_of(before, name)
        found[name, AFTER] = mean_of(after, name)
    found[MEMORISED, ACROSS] = difference(found[RECALL_GAIN, BEFORE], found[RECALL_GAIN, AFTER])
    found[INFERRED, AFTER] = found[TOOL_GAIN, AFTER]
    found[TOOLS_OVER_MEMORY, ACROSS] = difference(found[TOOL_GAIN, BEFORE], found[TOOL_GAIN, AFTER])
    return found


def quantile(values: Sequence[float], p: float) -> float:
    ordered = sorted(values)
    position = p * (len(ordered) - 1)
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


@dataclass(frozen=True)
class Estimate:
    component: str
    era: str
    instants: int
    value: float | None
    low: float | None = None
    high: float | None = None

    @property
    def interval(self) -> bool:
        return self.low is not None


def decompose(scored: Iterable[Scored], rounds: int = ROUNDS, seed: str = "decomposition"):
    scored = sorted(scored, key=lambda s: s.at)
    before = [s for s in scored if s.era == BEFORE]
    after = [s for s in scored if s.era == AFTER]
    point = statistics(before, after)
    sizes = {BEFORE: (len(before),), AFTER: (len(after),), ACROSS: (len(before), len(after))}
    drawn: dict[tuple[str, str], list[float]] = {k: [] for k in point}
    if max(len(before), len(after)) >= 2:
        rng = Random(seed)
        for _ in range(rounds):
            again = statistics(
                [rng.choice(before) for _ in before], [rng.choice(after) for _ in after]
            )
            for key, value in again.items():
                if value is not None:
                    drawn[key].append(value)
    tail = (1 - LEVEL) / 2
    found = []
    for (component, where), value in point.items():
        counts = sizes[where]
        if value is not None and min(counts) >= 2:
            low, high = (
                quantile(drawn[component, where], tail),
                quantile(drawn[component, where], 1 - tail),
            )
        else:
            low = high = None
        found.append(Estimate(component, where, sum(counts), value, low, high))
    return found

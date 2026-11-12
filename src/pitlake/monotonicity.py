import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import product
from random import Random

from pitlake.backtest import as_utc
from pitlake.harness import STRATEGY_DECIDER, SYMBOLS, View
from pitlake.information import Item

LEVELS = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
DRAWS = 5
DAYS = 30
ALPHA = 0.05
EXACT = 12
ROUNDS = 10000
STANCES = {"overweight": 1, "neutral": 0, "underweight": -1}
CLOSE = "close"


def ladder(found: Iterable[float]) -> tuple[float, ...]:
    levels = sorted(set(found))
    if len(levels) < 2 or levels[0] < 0 or levels[-1] > 1:
        raise ValueError("levels are at least two shares between 0 and 1")
    return tuple(levels)


def ladder_text(levels: Iterable[float]) -> str:
    return " ".join(f"{f:g}" for f in levels)


def percent(fraction: float) -> str:
    return f"{fraction * 100:g}%"


def shuffled(items: Iterable[Item], seed: str) -> tuple[str, ...]:
    found = sorted(i.id for i in items)
    Random(seed).shuffle(found)
    return tuple(found)


def kept_at(order: Sequence[str], fraction: float) -> frozenset[str]:
    return frozenset(order[: round(fraction * len(order))])


def middle(levels: Sequence[float]) -> float | None:
    inside = levels[1:-1]
    return min(inside, key=lambda f: (round(abs(f - 0.5), 9), f)) if inside else None


@dataclass(frozen=True)
class Outcome:
    returns: dict[str, float]
    start: datetime
    end: datetime


def outcome(closes: Iterable[tuple], at: datetime, days: int) -> Outcome:
    start, end = as_utc(at), as_utc(at) + timedelta(days=days)
    rows = [r for r in closes if r[1] == CLOSE and r[0] in SYMBOLS]
    before, after = View(rows, start), View(rows, end)
    traded = [s for s in SYMBOLS if any(r[0] == s for r in rows)]
    if not traded:
        raise ValueError("no symbol of the task has a close")
    found = {}
    for symbol in traded:
        first, last = before.latest(symbol, CLOSE), after.latest(symbol, CLOSE)
        if first is None or not first[4]:
            raise ValueError(f"no close of {symbol} was public at {start:%Y-%m-%d %H:%M} UTC")
        if not any(r[0] == symbol and r[3] > end for r in rows):
            raise ValueError(
                f"the outcome is not known yet: no close of {symbol} became public after "
                f"{end:%Y-%m-%d %H:%M} UTC"
            )
        found[symbol] = float(last[4]) / float(first[4]) - 1
    return Outcome(found, start, end)


def tilt(decider: str, decision: str | None) -> dict[str, float]:
    if decision is None:
        return {}
    found = json.loads(decision)
    if decider == STRATEGY_DECIDER:
        return {k: float(v) for k, v in found["weights"].items()}
    return {s: STANCES.get(found.get(s), 0) / len(SYMBOLS) for s in SYMBOLS}


def quality(tilts: dict[str, float], returns: dict[str, float]) -> float:
    mean = sum(returns.values()) / len(returns)
    return sum(w * (returns[s] - mean) for s, w in tilts.items() if s in returns)


def world_quality(decider: str, decisions: Iterable[str | None], returns: dict) -> float:
    found = [quality(tilt(decider, d), returns) for d in decisions]
    return sum(found) / len(found) if found else 0.0


def flips(diffs: Sequence[float], seed: str) -> tuple[float, float]:
    if not diffs:
        return 1.0, 1.0
    observed, sizes = sum(diffs), [abs(d) for d in diffs]
    tolerance = 1e-12 * (1 + sum(sizes))
    if len(sizes) <= EXACT:
        sums = [
            sum(s if up else -s for s, up in zip(sizes, signs, strict=True))
            for signs in product((True, False), repeat=len(sizes))
        ]
        below = sum(1 for s in sums if s <= observed + tolerance)
        above = sum(1 for s in sums if s >= observed - tolerance)
        return below / len(sums), above / len(sums)
    rng = Random(seed)
    below = above = 0
    for _ in range(ROUNDS):
        found = sum(s if rng.random() < 0.5 else -s for s in sizes)
        below += found <= observed + tolerance
        above += found >= observed - tolerance
    return (1 + below) / (1 + ROUNDS), (1 + above) / (1 + ROUNDS)


def holm(p_values: Sequence[float]) -> list[float]:
    ranked = sorted(range(len(p_values)), key=lambda i: (p_values[i], i))
    adjusted, running = [1.0] * len(p_values), 0.0
    for rank, i in enumerate(ranked):
        running = max(running, min(1.0, (len(p_values) - rank) * p_values[i]))
        adjusted[i] = running
    return adjusted


@dataclass(frozen=True)
class Pair:
    low: float
    high: float
    units: int
    before: float
    after: float
    falls: float
    rises: float
    falls_adjusted: float = 1.0
    rises_adjusted: float = 1.0

    @property
    def change(self) -> float:
        return self.after - self.before

    @property
    def fell(self) -> bool:
        return self.falls_adjusted < ALPHA

    @property
    def rose(self) -> bool:
        return self.rises_adjusted < ALPHA


def compare(chains: Sequence[dict[float, float]], levels: Sequence[float]) -> list[Pair]:
    complete = [c for c in chains if all(f in c for f in levels)]
    found = []
    for j, low in enumerate(levels):
        for high in levels[j + 1 :]:
            diffs = [c[high] - c[low] for c in complete]
            falls, rises = flips(diffs, f"{low:g}/{high:g}/{len(diffs)}")
            before = sum(c[low] for c in complete) / max(len(complete), 1)
            after = sum(c[high] for c in complete) / max(len(complete), 1)
            found.append(Pair(low, high, len(complete), before, after, falls, rises))
    falling = holm([p.falls for p in found])
    rising = holm([p.rises for p in found])
    return [
        Pair(p.low, p.high, p.units, p.before, p.after, p.falls, p.rises, f, r)
        for p, f, r in zip(found, falling, rising, strict=True)
    ]


def regions(pairs: Iterable[Pair]) -> list[tuple[float, float]]:
    merged: list[list[float]] = []
    for low, high in sorted((p.low, p.high) for p in pairs if p.fell):
        if merged and low <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], high)
        else:
            merged.append([low, high])
    return [(low, high) for low, high in merged]


def saturates(pairs: Sequence[Pair], levels: Sequence[float]) -> float:
    for level in levels:
        if not any(p.rose for p in pairs if p.low >= level):
            return level
    return levels[-1]


@dataclass(frozen=True)
class Verdict:
    decider: str
    decisions: int
    chains: int
    levels: tuple[float, ...]
    means: dict[float, float]
    pairs: tuple[Pair, ...]

    @property
    def regions(self) -> list[tuple[float, float]]:
        return regions(self.pairs)

    @property
    def monotonic(self) -> bool:
        return not self.regions

    @property
    def saturates(self) -> float:
        return saturates(self.pairs, self.levels)


def verdict(
    decider: str, decisions: int, chains: Sequence[dict[float, float]], levels: Sequence[float]
) -> Verdict:
    complete = [c for c in chains if all(f in c for f in levels)]
    means = {f: sum(c[f] for c in complete) / max(len(complete), 1) for f in levels}
    return Verdict(
        decider, decisions, len(complete), tuple(levels), means, tuple(compare(complete, levels))
    )

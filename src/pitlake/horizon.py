import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pitlake.attribution import SAME, Base, Effect, null_passed
from pitlake.backtest import as_utc
from pitlake.harness import Sample
from pitlake.information import Item

FULL = "full"
LADDER = "ladder"
REFINE = "refine"
HORIZONS = (365, 180, 120, 90, 60, 45, 30, 21, 14, 7, 3, 1, 0)
DAY = timedelta(days=1)


def cutoff(at: datetime, days: int | None) -> datetime | None:
    return None if days is None else as_utc(at) - days * DAY


def kept_within(items: Iterable[Item], at: datetime, days: int | None) -> frozenset[str]:
    start = cutoff(at, days)
    return frozenset(i.id for i in items if start is None or i.knowable_at > start)


def span(items: Iterable[Item], at: datetime) -> int:
    oldest = min((i.knowable_at for i in items), default=as_utc(at))
    return (as_utc(at) - oldest) // DAY + 1


def ladder(horizons: Iterable[int]) -> tuple[int, ...]:
    found = sorted(set(horizons), reverse=True)
    if not found or found[-1] < 0:
        raise ValueError("horizons are whole days, at least 0")
    return tuple(found)


@dataclass(frozen=True)
class Measured:
    reproduced: bool
    same: float
    agreement: float
    outcomes: dict[str, str]


def measure(effect: Effect, agreement: float | None = None) -> Measured:
    outcomes = dict(effect.outcomes)
    same = sum(1 for o in outcomes.values() if o == SAME) / max(len(outcomes), 1)
    return Measured(null_passed(effect), same, same if agreement is None else agreement, outcomes)


def agreement(base: Base, samples: list[Sample]) -> float:
    decided = [json.loads(s.decision) if s.valid else {} for s in samples]
    if not decided or not base.settled:
        return 0.0
    shares = [
        sum(1 for d in decided if d.get(part) == value) / len(decided)
        for part, value in base.settled.items()
    ]
    return sum(shares) / len(shares)


def screened(base: Base) -> float:
    shares = [base.share(part) for part in base.settled]
    return sum(shares) / len(shares) if shares else 0.0


@dataclass(frozen=True)
class Curve:
    points: tuple[tuple[int | None, str, Measured], ...]
    holds_to: int | None
    breaks_at: int | None

    @property
    def full(self) -> Measured:
        return self.points[0][2]

    @property
    def recovers(self) -> tuple[int, ...]:
        if self.breaks_at is None:
            return ()
        shorter = [(d, m) for d, _, m in self.points if d is not None and d < self.breaks_at]
        return tuple(d for d, m in shorter if m.reproduced)

    def parts(self) -> dict[str, int | None]:
        found: dict[str, int | None] = {}
        for part in self.full.outcomes:
            held = None
            for days, _, measured in self.points[1:]:
                if measured.outcomes.get(part) != SAME:
                    break
                held = days
            found[part] = held
        return found


Decides = Callable[[int | None, str], Measured]


def rank(days: int | None) -> tuple[bool, int]:
    return (days is not None, -(days or 0))


def ordered(points: Iterable[tuple[int | None, str, Measured]]) -> tuple:
    return tuple(sorted(points, key=lambda p: rank(p[0])))


def trace(horizons: Iterable[int], decides: Decides, longest: int) -> Curve:
    full = decides(None, FULL)
    points = [(None, FULL, full)]
    if not full.reproduced:
        return Curve(ordered(points), None, None)
    held, broke = None, None
    for days in ladder(horizons):
        measured = decides(days, LADDER)
        points.append((days, LADDER, measured))
        if broke is None:
            if measured.reproduced:
                held = days
            else:
                broke = days
    if broke is None:
        return Curve(ordered(points), held, None)
    high, low = (longest if held is None else held), broke
    while high - low > 1:
        middle = (high + low) // 2
        measured = decides(middle, REFINE)
        points.append((middle, REFINE, measured))
        if measured.reproduced:
            high = middle
        else:
            low = middle
    return Curve(ordered(points), high if high > low else None, low)

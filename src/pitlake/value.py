from collections.abc import Iterable
from dataclasses import dataclass

from pitlake.attribution import Effect, null_passed
from pitlake.backtest import fingerprint
from pitlake.information import BAR, FILING, RELEASE, Item

EDGAR = "edgar"
FRED = "fred"
FEED = "feed"
SERIES = "series"
LEVELS = (FEED, SERIES)
NONE = "none"
CHANGES = "CHANGES"
NOTHING = "CHANGES NOTHING"
INCONCLUSIVE = "INCONCLUSIVE"
EVERY = "all"


@dataclass(frozen=True)
class Source:
    name: str
    level: str
    feed: str
    items: frozenset[str]

    @property
    def withheld_hash(self) -> str:
        return withheld_hash(self.items)


def withheld_hash(items: Iterable[str]) -> str:
    return fingerprint(sorted(set(items)))


def origin(item: Item, venues: dict[str, str]) -> tuple[str, str]:
    if item.kind == BAR:
        if item.entity not in venues:
            raise ValueError(f"{item.id}: no venue trades {item.entity}")
        feed = venues[item.entity]
        return feed, f"{feed}/{item.entity}"
    if item.kind == FILING:
        return EDGAR, f"{EDGAR}/{item.entity}"
    if item.kind == RELEASE:
        return FRED, f"{FRED}/{item.key.split('/')[0]}"
    raise ValueError(f"{item.id}: no source for an item of kind {item.kind}")


def sources(
    items: Iterable[Item], venues: dict[str, str], levels: Iterable[str] = LEVELS
) -> list[Source]:
    levels = tuple(levels)
    unknown = sorted(set(levels) - set(LEVELS))
    if unknown or not levels:
        raise ValueError(f"levels are {' and/or '.join(LEVELS)}, not {unknown}")
    feeds: dict[str, set[str]] = {}
    series: dict[str, tuple[str, set[str]]] = {}
    for item in items:
        feed, name = origin(item, venues)
        feeds.setdefault(feed, set()).add(item.id)
        series.setdefault(name, (feed, set()))[1].add(item.id)
    found = []
    if FEED in levels:
        found += [Source(f, FEED, f, frozenset(i)) for f, i in sorted(feeds.items())]
    if SERIES in levels:
        for name, (feed, ids) in sorted(series.items()):
            if FEED in levels and ids == feeds[feed]:
                continue
            found.append(Source(name, SERIES, feed, frozenset(ids)))
    return found


@dataclass(frozen=True)
class Tally:
    attributed: int = 0
    inputs_changed: int = 0
    caused: int = 0
    parts: frozenset[str] = frozenset()

    def __add__(self, other: "Tally") -> "Tally":
        return Tally(
            self.attributed + other.attributed,
            self.inputs_changed + other.inputs_changed,
            self.caused + other.caused,
            self.parts | other.parts,
        )


def tally(records: Iterable, source: Source) -> Tally:
    found = Tally()
    for record in records:
        if len(record.withheld) != 1 or record.withheld[0] not in source.items:
            continue
        caused = tuple(record.effect.caused)
        found += Tally(1, int(bool(record.inputs_changed)), int(bool(caused)), frozenset(caused))
    return found


def largest(effect: Effect) -> float:
    return max((abs(v) for v in effect.effects.values()), default=0.0)


def judge(effects: Iterable[Effect]) -> str:
    effects = list(effects)
    if not effects:
        return INCONCLUSIVE
    if any(e.caused for e in effects):
        return CHANGES
    return NOTHING if all(null_passed(e) for e in effects) else INCONCLUSIVE


@dataclass(frozen=True)
class Value:
    decider: str
    source: Source
    decisions: int
    changed: int
    parts: int
    parts_changed: int
    largest: float
    knowable: int
    one_at_a_time: Tally
    verdict: str


def value(
    decider: str,
    source: Source,
    effects: list[Effect],
    knowable: int,
    one_at_a_time: Tally,
) -> Value:
    return Value(
        decider,
        source,
        len(effects),
        sum(1 for e in effects if e.caused),
        sum(len(e.outcomes) for e in effects),
        sum(len(e.caused) for e in effects),
        max((largest(e) for e in effects), default=0.0),
        knowable,
        one_at_a_time,
        judge(effects),
    )


def combine(values: list[Value]) -> Value:
    verdicts = {v.verdict for v in values}
    if CHANGES in verdicts:
        verdict = CHANGES
    elif verdicts == {NOTHING}:
        verdict = NOTHING
    else:
        verdict = INCONCLUSIVE
    total = Tally()
    for v in values:
        total += v.one_at_a_time
    return Value(
        EVERY,
        values[0].source,
        sum(v.decisions for v in values),
        sum(v.changed for v in values),
        sum(v.parts for v in values),
        sum(v.parts_changed for v in values),
        max(v.largest for v in values),
        max(v.knowable for v in values),
        total,
        verdict,
    )

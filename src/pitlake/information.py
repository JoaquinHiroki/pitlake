import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pitlake.backtest import as_utc, fingerprint, instant
from pitlake.gold import (
    GLOBAL_ENTITY,
    MACRO_SERIES,
    PRICE_FEATURES,
    WINDOW_DAYS,
    YEAR_OVER_YEAR,
    feature_name,
)
from pitlake.silver import EDGAR_COMPANY_FACTS_SILVER, FRED_SERIES_VINTAGES_SILVER

BAR = "bar"
FILING = "filing"
RELEASE = "release"
KINDS = (BAR, FILING, RELEASE)
READ = "read"
UNREAD = "unread"
PRICE_SOURCE = "bars_1d"
FILING_SOURCE = EDGAR_COMPANY_FACTS_SILVER.table
RELEASE_SOURCE = FRED_SERIES_VINTAGES_SILVER.table
LEVELS = {name: series for series, name in MACRO_SERIES.items()}
SERIES_OF = {**LEVELS, **{name: series for series, name in YEAR_OVER_YEAR.items()}}
ITEM_ID = re.compile(r"(bar|filing|release):[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)?")
SAMPLE = 20

WORLD_SCHEMA = (
    "entity STRING, feature STRING, event_time TIMESTAMP, knowable_at TIMESTAMP, value DOUBLE, "
    "source_table STRING, source_ref STRING"
)


def item_id(kind: str, *parts: str) -> str:
    if kind not in KINDS:
        raise ValueError(f"{kind!r}: an item is one of {KINDS}")
    return f"{kind}:{'/'.join(parts)}"


def parse_item_id(text: str) -> str:
    text = text.strip()
    if not ITEM_ID.fullmatch(text):
        raise ValueError(f"{text!r} is not an item ID such as filing:0000320193-25-000008")
    return text


@dataclass(frozen=True)
class Reach:
    entities: tuple[str, ...]
    features: tuple[str, ...]

    @property
    def prices(self) -> bool:
        return any(f in PRICE_FEATURES for f in self.features)

    @property
    def series(self) -> tuple[str, ...]:
        return tuple(sorted({SERIES_OF[f] for f in self.features if f in SERIES_OF}))

    @property
    def levels(self) -> tuple[str, ...]:
        return tuple(sorted(name for name, series in LEVELS.items() if series in self.series))


@dataclass(frozen=True)
class Item:
    kind: str
    key: str
    entity: str
    knowable_at: datetime
    digest: str

    @property
    def id(self) -> str:
        return f"{self.kind}:{self.key}"


def owner(entity: str, feature: str, source_table: str, source_ref: str) -> str:
    if source_table == PRICE_SOURCE:
        return item_id(BAR, entity, source_ref)
    if source_table == FILING_SOURCE:
        return item_id(FILING, source_ref)
    if source_table == RELEASE_SOURCE and feature in SERIES_OF:
        return item_id(RELEASE, SERIES_OF[feature], source_ref)
    raise ValueError(f"no item owns {feature} of {entity} from {source_table} {source_ref}")


def sessions(items: Iterable[Item]) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for item in items:
        if item.kind == BAR:
            found.setdefault(item.entity, []).append(item.key.split("/")[1])
    return {entity: sorted(days) for entity, days in found.items()}


def lineage(
    entity: str, feature: str, source_table: str, source_ref: str, days: dict[str, list[str]]
) -> tuple[str, ...]:
    if source_table != PRICE_SOURCE:
        return (owner(entity, feature, source_table, source_ref),)
    if feature not in PRICE_FEATURES:
        raise ValueError(f"{feature} of {entity} is not a price feature")
    known = days.get(entity, [])
    if source_ref not in known:
        raise ValueError(f"{feature} of {entity} for {source_ref} has no daily bar")
    end = known.index(source_ref)
    start = max(0, end - WINDOW_DAYS)
    return tuple(item_id(BAR, entity, day) for day in known[start : end + 1])


def bar_items(rows: Iterable[tuple], reach: Reach) -> list[Item]:
    if not reach.prices:
        return []
    found: dict[str, Item] = {}
    for venue, symbol, day, *prices, knowable_at in rows:
        if symbol not in reach.entities:
            continue
        knowable_at = as_utc(knowable_at)
        item = Item(
            BAR,
            f"{symbol}/{day}",
            symbol,
            knowable_at,
            fingerprint([venue, symbol, day, *prices, knowable_at.isoformat()]),
        )
        if item.id in found:
            raise ValueError(f"two daily bars are {item.id}; a symbol must trade on one venue")
        found[item.id] = item
    return list(found.values())


def filing_items(rows: Iterable[tuple], reach: Reach) -> list[Item]:
    grouped: dict[str, list[tuple]] = {}
    for row in rows:
        if row[0] in reach.entities:
            grouped.setdefault(row[1], []).append(row)
    found = []
    for accession, reported in sorted(grouped.items()):
        symbols = {r[0] for r in reported}
        if len(symbols) > 1:
            raise ValueError(f"filing {accession} is reported for {sorted(symbols)}")
        if not any(feature_name(r[2], r[3]) in reach.features for r in reported):
            continue
        times = [as_utc(r[-1]) for r in reported]
        content = sorted(
            [*(None if v is None else str(v) for v in r[2:-1]), t.isoformat()]
            for r, t in zip(reported, times, strict=True)
        )
        found.append(Item(FILING, accession, reported[0][0], min(times), fingerprint(content)))
    return found


def release_items(rows: Iterable[tuple], reach: Reach) -> list[Item]:
    grouped: dict[tuple[str, str], list[tuple]] = {}
    for _, feature, event_time, knowable_at, value, _, vintage in rows:
        if feature in reach.levels:
            grouped.setdefault((LEVELS[feature], vintage), []).append(
                (as_utc(event_time), as_utc(knowable_at), value)
            )
    return [
        Item(
            RELEASE,
            f"{series}/{vintage}",
            GLOBAL_ENTITY,
            min(r[1] for r in observed),
            fingerprint(sorted([e.isoformat(), v] for e, _, v in observed)),
        )
        for (series, vintage), observed in sorted(grouped.items())
    ]


@dataclass(frozen=True)
class InformationSet:
    at: datetime
    items: dict[str, Item]
    later: int
    omitted: tuple[str, ...]

    @property
    def hash(self) -> str:
        return fingerprint(
            sorted([i.id, i.knowable_at.isoformat(), i.digest] for i in self.items.values())
        )

    def by_kind(self) -> dict[str, int]:
        counts = Counter(i.kind for i in self.items.values())
        return {kind: counts.get(kind, 0) for kind in KINDS}


def information_set(
    items: Iterable[Item], at: datetime, omit: Iterable[str] = ()
) -> InformationSet:
    at = as_utc(at)
    items = list(items)
    ids = [i.id for i in items]
    repeated = sorted({i for i in ids if ids.count(i) > 1})
    if repeated:
        raise ValueError(f"items enumerated twice: {repeated[:5]}")
    known = {i.id: i for i in items if i.knowable_at <= at}
    omitted = tuple(dict.fromkeys(omit))
    missing = [o for o in omitted if o not in known]
    if missing:
        raise ValueError(f"cannot omit {missing}: not knowable at {at.isoformat()}")
    for gone in omitted:
        del known[gone]
    later = sum(1 for i in items if i.knowable_at > at)
    return InformationSet(at, known, later, omitted)


def world(rows: Iterable[tuple], info: InformationSet) -> list[tuple]:
    kept = []
    for row in rows:
        entity, feature, _, _, _, source_table, source_ref = row
        try:
            if owner(entity, feature, source_table, source_ref) in info.items:
                kept.append(row)
        except ValueError:
            continue
    return kept


@dataclass(frozen=True)
class Accounting:
    read: frozenset[str]
    owned: dict[str, int]
    values: int
    unaccounted: int
    sample: tuple[str, ...]

    def status(self, item: str) -> str:
        return READ if item in self.read else UNREAD


def account(
    info: InformationSet,
    rows: Iterable[tuple],
    received: Iterable[tuple],
    days: dict[str, list[str]],
) -> Accounting:
    problems = []
    owned: Counter = Counter()
    for entity, feature, event_time, knowable_at, _, source_table, source_ref in rows:
        knowable_at = as_utc(knowable_at)
        try:
            found = owner(entity, feature, source_table, source_ref)
        except ValueError as error:
            if knowable_at <= info.at:
                problems.append(str(error))
            continue
        item = info.items.get(found)
        if item is None:
            if knowable_at <= info.at:
                problems.append(
                    f"{feature} of {entity} for {as_utc(event_time):%Y-%m-%d} was knowable at "
                    f"{knowable_at.isoformat()} and comes from {found}, which is not in the set"
                )
            continue
        owned[found] += 1
        if knowable_at < item.knowable_at:
            problems.append(
                f"{feature} of {entity} for {as_utc(event_time):%Y-%m-%d} was knowable at "
                f"{knowable_at.isoformat()}, before {found} at {item.knowable_at.isoformat()}"
            )
    read: set[str] = set()
    values = 0
    for entity, at, feature, _, _, source_table, source_ref in received:
        values += 1
        try:
            needed = lineage(entity, feature, source_table, source_ref, days)
        except ValueError as error:
            problems.append(str(error))
            continue
        outside = [i for i in needed if i not in info.items]
        if outside:
            problems.append(
                f"{feature} of {entity} seen at {as_utc(at).isoformat()} depends on "
                f"{', '.join(outside[:3])}, outside the set"
            )
        read.update(i for i in needed if i in info.items)
    return Accounting(frozenset(read), dict(owned), values, len(problems), tuple(problems[:SAMPLE]))


def _text(column: str):
    from pyspark.sql import functions as F

    return F.col(column).cast("string").alias(column)


def _micros(column: str):
    from pyspark.sql import functions as F

    return F.unix_micros(column).alias(column)


def bar_rows(frame) -> list[tuple]:
    columns = ("session_date", "open", "high", "low", "close", "volume")
    rows = frame.select("venue", "symbol", *map(_text, columns), _micros("knowable_at")).collect()
    return [(*r[:-1], instant(r[-1])) for r in rows]


def filing_rows(frame) -> list[tuple]:
    columns = ("period_start", "period_end", "value", "unit", "concept", "form", "filed_date")
    rows = frame.select(
        "symbol",
        "accession_number",
        "metric",
        "duration",
        *map(_text, columns),
        _micros("knowable_at"),
    ).collect()
    return [(*r[:-1], instant(r[-1])) for r in rows]


def feature_rows(frame) -> list[tuple]:
    rows = frame.select(
        "entity",
        "feature",
        _micros("event_time"),
        _micros("knowable_at"),
        "value",
        "source_table",
        "source_ref",
    ).collect()
    return [(r[0], r[1], instant(r[2]), instant(r[3]), r[4], r[5], r[6]) for r in rows]


def in_reach(features, reach: Reach):
    from pyspark.sql import functions as F

    return features.where(
        F.col("entity").isin(*reach.entities) & F.col("feature").isin(*reach.features)
    )


def release_sources(features, reach: Reach):
    from pyspark.sql import functions as F

    if not reach.levels:
        return features.limit(0)
    return features.where((F.col("entity") == GLOBAL_ENTITY) & F.col("feature").isin(*reach.levels))


def enumerate_items(bars, fundamentals, features, reach: Reach) -> list[Item]:
    from pyspark.sql import functions as F

    symbols = [e for e in reach.entities if e != GLOBAL_ENTITY]
    return (
        bar_items(bar_rows(bars.where(F.col("symbol").isin(*symbols))), reach)
        + filing_items(filing_rows(fundamentals.where(F.col("symbol").isin(*symbols))), reach)
        + release_items(feature_rows(release_sources(features, reach)), reach)
    )

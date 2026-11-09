from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

from pitlake.gold import GLOBAL_ENTITY, WINDOW_DAYS, fundamental_features, macro_features
from pitlake.gold import price_features as gold_price_features
from pitlake.information import (
    BAR,
    FILING,
    LEVELS,
    PRICE_SOURCE,
    RELEASE,
    Reach,
    feature_rows,
    in_reach,
    lineage,
    owner,
)

COHERENT = "coherent"
HIDE = "hide"
MODES = (COHERENT, HIDE)
SAMPLE = 5


def split(item: str) -> tuple[str, str]:
    kind, key = item.split(":", 1)
    return kind, key


def withheld_bars(withheld: Iterable[str]) -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for item in withheld:
        kind, key = split(item)
        if kind == BAR:
            symbol, day = key.split("/")
            found.setdefault(symbol, set()).add(day)
    return found


def without_bars(days: dict[str, list[str]], withheld: Iterable[str]) -> dict[str, list[str]]:
    gone = withheld_bars(withheld)
    return {s: [d for d in found if d not in gone.get(s, ())] for s, found in days.items()}


@dataclass(frozen=True)
class Plan:
    withheld: tuple[str, ...]
    removed: tuple[tuple, ...]
    recompute: dict[str, tuple[tuple[str, str], ...]]
    inputs: dict[str, tuple[str, ...]]


def plan(rows: Iterable[tuple], withheld: Iterable[str], days: dict[str, list[str]]) -> Plan:
    withheld = tuple(sorted(set(withheld)))
    gone = set(withheld)
    bars = withheld_bars(withheld)
    removed, recompute = [], {}
    for row in rows:
        entity, feature, _, _, _, source_table, source_ref = row
        if owner(entity, feature, source_table, source_ref) in gone:
            removed.append(row)
        elif source_table == PRICE_SOURCE and entity in bars:
            if gone & set(lineage(entity, feature, source_table, source_ref, days)):
                removed.append(row)
                recompute.setdefault(entity, set()).add((feature, source_ref))
    left = without_bars(days, withheld)
    inputs = {}
    for symbol, needed in recompute.items():
        kept = left[symbol]
        positions = [kept.index(d) for d in {day for _, day in needed}]
        start = max(0, min(positions) - WINDOW_DAYS - 1)
        inputs[symbol] = tuple(kept[start : max(positions) + 1])
    return Plan(
        withheld,
        tuple(removed),
        {s: tuple(sorted(d)) for s, d in recompute.items()},
        inputs,
    )


def hidden(rows: Iterable[tuple], withheld: Iterable[str]) -> Plan:
    withheld = tuple(sorted(set(withheld)))
    gone = set(withheld)
    removed = tuple(r for r in rows if owner(r[0], r[1], r[5], r[6]) in gone)
    return Plan(withheld, removed, {}, {})


def apply(rows: Iterable[tuple], removal: Plan, recomputed: Iterable[tuple]) -> list[tuple]:
    gone = Counter(removal.removed)
    kept = []
    for row in rows:
        if gone[row]:
            gone[row] -= 1
        else:
            kept.append(row)
    return kept + list(recomputed)


def differences(world: Iterable[tuple], reference: Iterable[tuple]) -> tuple[int, int, list]:
    have, want = Counter(world), Counter(reference)
    extra, missing = have - want, want - have
    sample = [("extra", r) for r in list(extra)[:3]] + [("missing", r) for r in list(missing)[:3]]
    return sum(extra.values()), sum(missing.values()), sample


def owned_left(world: Iterable[tuple], withheld: Iterable[str]) -> int:
    gone = set(withheld)
    return sum(1 for r in world if owner(r[0], r[1], r[5], r[6]) in gone)


def sample_items(items: Iterable, read: frozenset[str], per_kind: int) -> list[str]:
    chosen = []
    for kind in (BAR, FILING, RELEASE):
        found = [i for i in items if i.kind == kind and i.id in read]
        found.sort(key=lambda i: (i.knowable_at, i.id))
        if per_kind <= 0 or not found:
            continue
        if len(found) <= per_kind:
            picks = range(len(found))
        elif per_kind == 1:
            picks = [len(found) // 2]
        else:
            picks = sorted({round(k * (len(found) - 1) / (per_kind - 1)) for k in range(per_kind)})
        chosen += [found[k].id for k in picks]
    return chosen


def _matches(column, items: Iterable[str]):
    from pyspark.sql import functions as F

    items = sorted(items)
    return F.lit(False) if not items else column.isin(*items)


def vintages(features):
    from pyspark.sql import functions as F

    series = F.create_map(*(F.lit(x) for pair in LEVELS.items() for x in pair))
    return features.where(
        (F.col("entity") == GLOBAL_ENTITY) & F.col("feature").isin(*LEVELS)
    ).select(
        series[F.col("feature")].alias("series_id"),
        F.to_date("event_time").alias("observation_date"),
        F.to_date("source_ref").alias("vintage_date"),
        F.col("value"),
        F.col("knowable_at"),
    )


def rebuild(bars, fundamentals, features, withheld: Iterable[str], reach: Reach) -> list[tuple]:
    from pyspark.sql import functions as F

    withheld = set(withheld)
    symbols = [e for e in reach.entities if e != GLOBAL_ENTITY]
    bar_id = F.concat(F.lit(f"{BAR}:"), "symbol", F.lit("/"), F.col("session_date").cast("string"))
    filing_id = F.concat(F.lit(f"{FILING}:"), "accession_number")
    release_id = F.concat(
        F.lit(f"{RELEASE}:"), "series_id", F.lit("/"), F.col("vintage_date").cast("string")
    )
    kept_bars = bars.where(F.col("symbol").isin(*symbols) & ~_matches(bar_id, withheld))
    kept_facts = fundamentals.where(F.col("symbol").isin(*symbols) & ~_matches(filing_id, withheld))
    kept_vintages = vintages(features).where(~_matches(release_id, withheld))
    built = (
        gold_price_features(kept_bars)
        .unionByName(fundamental_features(kept_facts))
        .unionByName(macro_features(kept_vintages))
    )
    return feature_rows(in_reach(built, reach))


def recompute(bars, removal: Plan, reach: Reach) -> list[tuple]:
    from pyspark.sql import functions as F

    found = []
    for symbol, days in sorted(removal.inputs.items()):
        window = bars.where(
            (F.col("symbol") == symbol) & F.col("session_date").cast("string").isin(*days)
        )
        pairs = [f"{feature}|{day}" for feature, day in removal.recompute[symbol]]
        computed = gold_price_features(window).where(
            F.concat("feature", F.lit("|"), "source_ref").isin(*pairs)
        )
        found += feature_rows(in_reach(computed, reach))
    return found

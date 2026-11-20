import gzip
import json
from collections.abc import Iterable
from datetime import datetime, timedelta

from pitlake.backtest import EPOCH, as_utc, fingerprint
from pitlake.gold import GLOBAL_ENTITY, PRICE_FEATURES, WINDOW_DAYS
from pitlake.harness import AGENT_REACH, DAYS, HARNESS_REACH, SYMBOLS, View, agent_tools, in_reach
from pitlake.information import (
    BAR,
    FILING,
    FILING_SOURCE,
    LEVELS,
    PRICE_SOURCE,
    RELEASE,
    RELEASE_SOURCE,
    SERIES_OF,
    Item,
    item_id,
    owner,
)

FORMAT = "pitlake-world/1"
RELEASES_VOLUME = "releases"
MICROSECOND = timedelta(microseconds=1)
LINEAGE = {
    "prices": PRICE_SOURCE,
    "filings": FILING_SOURCE,
    "releases": RELEASE_SOURCE,
    "window_days": WINDOW_DAYS,
    "price_features": list(PRICE_FEATURES),
    "series_of": dict(SERIES_OF),
    "levels": dict(LEVELS),
}


def micros(at: datetime) -> int:
    return (as_utc(at) - EPOCH) // MICROSECOND


def _row(r: tuple) -> list:
    entity, feature, event_time, knowable_at, value, table, ref = r
    return [entity, feature, micros(event_time), micros(knowable_at), value, table, ref]


def name_of(at: datetime, backtest_id: str) -> str:
    return f"world-{as_utc(at):%Y-%m-%d}-{backtest_id[:8]}"


def payload(
    at: datetime,
    items: dict[str, Item],
    bars: Iterable[tuple],
    filings: Iterable[tuple],
    releases: Iterable[tuple],
    rows: Iterable[tuple],
    set_hash: str,
    source: dict,
) -> dict:
    at = as_utc(at)
    kept_bars = [[*r[:-1], micros(r[-1])] for r in bars if item_id(BAR, r[1], str(r[2])) in items]
    kept_filings = [[*r[:-1], micros(r[-1])] for r in filings if item_id(FILING, r[1]) in items]
    kept_releases = [
        [r[1], micros(r[2]), micros(r[3]), r[4], r[6]]
        for r in releases
        if r[0] == GLOBAL_ENTITY
        and r[1] in LEVELS
        and item_id(RELEASE, LEVELS[r[1]], r[6]) in items
    ]
    world = owned_rows(rows, at)
    found = {
        "format": FORMAT,
        "name": name_of(at, source.get("backtest_id", "") or "unknown"),
        "at": micros(at),
        "source": source,
        "symbols": list(SYMBOLS),
        "global_entity": GLOBAL_ENTITY,
        "features": list(HARNESS_REACH.features),
        "lineage": LINEAGE,
        "set_hash": set_hash,
        "bars": sorted(kept_bars, key=lambda r: (r[1], r[2])),
        "filings": sorted(kept_filings, key=repr),
        "releases": sorted(kept_releases, key=repr),
        "rows": sorted(world, key=repr),
    }
    found["id"] = fingerprint(found)
    return found


def encode(found: dict) -> bytes:
    return gzip.compress(json.dumps(found, separators=(",", ":")).encode(), mtime=0)


def owned_rows(rows: Iterable[tuple], at: datetime) -> list[list]:
    at = as_utc(at)
    return [_row(r) for r in rows if as_utc(r[3]) <= at]


def unowned(rows: Iterable[tuple], items: dict[str, Item], at: datetime) -> list[str]:
    found = []
    for r in rows:
        if as_utc(r[3]) > as_utc(at):
            continue
        try:
            mine = owner(r[0], r[1], r[5], r[6])
        except ValueError as error:
            found.append(str(error))
            continue
        if mine not in items:
            found.append(f"{r[1]} of {r[0]} for {r[6]} comes from {mine}, not in the set")
    return found


def tool_calls() -> list[tuple[str, dict]]:
    calls = [("macro", {})]
    for symbol in SYMBOLS:
        calls += [
            ("symbol_snapshot", {"symbol": symbol}),
            ("price_history", {"symbol": symbol, "days": DAYS[1]}),
        ]
    return calls


def tool_texts(rows: Iterable[tuple], at: datetime) -> dict[str, str]:
    tools = agent_tools(View(in_reach(rows, AGENT_REACH), at))
    return {f"{n} {json.dumps(a, sort_keys=True)}": tools[n](a) for n, a in tool_calls()}

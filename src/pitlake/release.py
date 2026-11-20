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
from pitlake.value import EDGAR, FRED

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
RESTRICTED = "not for redistribution"
SHARE_ALIKE = "CC-BY-NC-SA-4.0"
PUBLIC = "public domain"
TERMS = {
    "alpaca": (
        RESTRICTED,
        "Alpaca Securities LLC market data (SIP daily bars): not to be reproduced or distributed "
        "without Alpaca's written consent.",
    ),
    "binance": (
        SHARE_ALIKE,
        "Binance Vision (data.binance.vision) under CC BY-NC-SA 4.0: non-commercial use, credit "
        "Binance Vision and share derivatives under the same licence.",
    ),
    EDGAR: (PUBLIC, "U.S. Securities and Exchange Commission, EDGAR."),
    FRED: (
        PUBLIC,
        "U.S. Bureau of Labor Statistics (UNRATE, CPIAUCSL) and Board of Governors of the Federal "
        "Reserve System (FEDFUNDS), retrieved from FRED, Federal Reserve Bank of St. Louis.",
    ),
}
STRICTNESS = (RESTRICTED, SHARE_ALIKE, PUBLIC)


def micros(at: datetime) -> int:
    return (as_utc(at) - EPOCH) // MICROSECOND


def _row(r: tuple) -> list:
    entity, feature, event_time, knowable_at, value, table, ref = r
    return [entity, feature, micros(event_time), micros(knowable_at), value, table, ref]


def name_of(at: datetime, backtest_id: str, without: Iterable[str] = ()) -> str:
    suffix = "".join(f"-without-{f}" for f in sorted(without))
    return f"world-{as_utc(at):%Y-%m-%d}-{backtest_id[:8]}{suffix}"


def terms(feeds: Iterable[str]) -> dict:
    feeds = sorted(set(feeds))
    unknown = [f for f in feeds if f not in TERMS]
    if unknown:
        raise ValueError(f"no terms recorded for {unknown}")
    licences = {TERMS[f][0] for f in feeds}
    licence = next((s for s in STRICTNESS if s in licences), PUBLIC)
    return {"licence": licence, "sources": {f: TERMS[f][1] for f in feeds}}


def payload(
    at: datetime,
    items: dict[str, Item],
    bars: Iterable[tuple],
    filings: Iterable[tuple],
    releases: Iterable[tuple],
    rows: Iterable[tuple],
    set_hash: str,
    source: dict,
    without: Iterable[str] = (),
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
    feeds = {b[0] for b in kept_bars}
    feeds |= {EDGAR} if kept_filings else set()
    feeds |= {FRED} if kept_releases else set()
    found = {
        "format": FORMAT,
        "name": name_of(at, source.get("backtest_id", "") or "unknown", without),
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
        "terms": terms(feeds),
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

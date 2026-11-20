import json
import math
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pitlake_counterfactual.world import (
    FORMAT,
    Bar,
    Lineage,
    bar_items,
    day_micros,
    filing_items,
    fingerprint,
    micros,
    price_rows,
    release_items,
    set_hash,
)

AT = datetime(2025, 2, 1, 6, tzinfo=UTC)
SYMBOLS = ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")
GLOBAL = "US"
FEATURES = (
    "close",
    "volatility_20d",
    "net_income_q",
    "cpi_yoy",
    "revenue_q",
    "eps_diluted_q",
    "unrate",
    "fedfunds",
)
LINEAGE = {
    "prices": "bars_1d",
    "filings": "edgar_company_facts",
    "releases": "fred_series_vintages",
    "window_days": 20,
    "price_features": ["close", "return_1d", "volatility_20d", "dollar_volume_20d"],
    "series_of": {
        "unrate": "UNRATE",
        "cpi": "CPIAUCSL",
        "fedfunds": "FEDFUNDS",
        "cpi_yoy": "CPIAUCSL",
    },
    "levels": {"unrate": "UNRATE", "cpi": "CPIAUCSL", "fedfunds": "FEDFUNDS"},
}
FILING = "0000320193-25-000008"
FILED = datetime(2025, 1, 31, 21, 30, tzinfo=UTC)
LAST_AAPL = "bar:AAPL/2025-01-31"


def sessions(symbol: str) -> list[date]:
    days = [date(2024, 12, 16) + timedelta(days=k) for k in range(47)]
    if symbol == "BTCUSDT":
        return days
    return [d for d in days if d.weekday() < 5]


def close(symbol: str, k: int, n: int) -> float:
    if symbol == "AAPL":
        if k == n - 1:
            return 260.0
        return 230.0 + k if k < n - 8 else 230.0 + (n - 8) - 3.0 * (k - (n - 8))
    base = {"BTCUSDT": 95000.0, "SPY": 590.0, "MSFT": 420.0, "NVDA": 135.0}[symbol]
    return round(base * (1 + 0.01 * math.sin(k / 3) + 0.002 * k), 4)


def bars() -> list[list]:
    found = []
    for symbol in SYMBOLS:
        days = sessions(symbol)
        venue = "binance" if symbol == "BTCUSDT" else "alpaca"
        lag = timedelta(days=1) if symbol == "BTCUSDT" else timedelta(days=1, hours=5)
        for k, day in enumerate(days):
            c = close(symbol, k, len(days))
            knowable = datetime.combine(day, datetime.min.time(), UTC) + lag
            prices = [f"{c * 0.99:.10f}", f"{c * 1.01:.10f}", f"{c * 0.98:.10f}", f"{c:.10f}"]
            found.append(
                [venue, symbol, day.isoformat(), *prices, f"{1000 + 10 * k:.10f}", micros(knowable)]
            )
    return found


def filings() -> list[list]:
    facts = (
        ("revenue", "quarter", "2024-09-29", "124300000000.000000", "USD", "Revenues"),
        ("net_income", "quarter", "2024-09-29", "36330000000.000000", "USD", "NetIncomeLoss"),
        (
            "eps_diluted",
            "quarter",
            "2024-09-29",
            "2.400000",
            "USD/shares",
            "EarningsPerShareDiluted",
        ),
    )
    return [
        [
            "AAPL",
            FILING,
            metric,
            duration,
            start,
            "2024-12-28",
            value,
            unit,
            concept,
            "10-Q",
            "2025-01-31",
            micros(FILED),
        ]
        for metric, duration, start, value, unit, concept in facts
    ]


def releases() -> list[list]:
    def at(*parts) -> int:
        return micros(datetime(*parts, tzinfo=UTC))

    return [
        ["unrate", day_micros("2024-11-01"), at(2025, 1, 10, 13, 30), 4.2, "2025-01-10"],
        ["unrate", day_micros("2024-12-01"), at(2025, 1, 10, 13, 30), 4.1, "2025-01-10"],
        ["cpi", day_micros("2023-12-01"), at(2025, 1, 15, 13, 30), 310.0, "2025-01-15"],
        ["cpi", day_micros("2024-12-01"), at(2025, 1, 15, 13, 30), 318.0, "2025-01-15"],
        ["fedfunds", day_micros("2024-12-01"), at(2025, 1, 2, 21, 0), 4.48, "2025-01-02"],
    ]


def rows(found_bars: list[list], found_filings: list[list], found_releases: list[list]) -> list:
    lineage = Lineage.of(LINEAGE)
    parsed = [Bar(r[0], r[1], r[2], tuple(r[3:-1]), r[-1]) for r in found_bars]
    found = []
    for symbol in SYMBOLS:
        mine = [b for b in parsed if b.symbol == symbol]
        found += [list(r) for r in price_rows(mine, lineage) if r[1] in FEATURES]
    suffix = {"quarter": "_q"}
    for symbol, accession, metric, duration, _, end, value, *_, knowable in found_filings:
        feature = metric + suffix[duration]
        found.append(
            [symbol, feature, day_micros(end), knowable, float(value), lineage.filings, accession]
        )
    for feature, event, knowable, value, vintage in found_releases:
        if feature in FEATURES:
            found.append([GLOBAL, feature, event, knowable, value, lineage.releases, vintage])
    cpi = {r[1]: r for r in found_releases if r[0] == "cpi"}
    latest = cpi[day_micros("2024-12-01")]
    earlier = cpi[day_micros("2023-12-01")]
    found.append(
        [
            GLOBAL,
            "cpi_yoy",
            latest[1],
            max(latest[2], earlier[2]),
            latest[3] / earlier[3] - 1,
            lineage.releases,
            latest[4],
        ]
    )
    return found


def payload() -> dict:
    found_bars, found_filings, found_releases = bars(), filings(), releases()
    lineage = Lineage.of(LINEAGE)
    parsed = [Bar(r[0], r[1], r[2], tuple(r[3:-1]), r[-1]) for r in found_bars]
    items = (
        bar_items(parsed)
        + filing_items(found_filings)
        + release_items(found_releases, lineage.levels, GLOBAL)
    )
    found = {
        "format": FORMAT,
        "name": "world-2025-02-01-fixture",
        "at": micros(AT),
        "source": {"catalog": "fixture"},
        "symbols": list(SYMBOLS),
        "global_entity": GLOBAL,
        "features": list(FEATURES),
        "lineage": LINEAGE,
        "set_hash": set_hash(items),
        "bars": found_bars,
        "filings": found_filings,
        "releases": found_releases,
        "rows": rows(found_bars, found_filings, found_releases),
    }
    found["id"] = fingerprint(found)
    return found


def write(path: Path) -> Path:
    path = Path(path)
    path.write_text(json.dumps(payload()))
    return path


if __name__ == "__main__":
    print(write(Path(sys.argv[1])))

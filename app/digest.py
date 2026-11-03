from dataclasses import dataclass
from datetime import UTC, datetime

import console

READS = ("gold.bars_1d", "gold.features", "gold.fundamentals")
LOOKBACK_DAYS = 180
AHEAD_DAYS = 60
MOVES = (1, 5, 20, 60)
KNOWN_FILINGS = 4


class LeakageFound(ValueError):
    pass


@dataclass(frozen=True)
class Symbol:
    venue: str
    name: str


SYMBOLS = {
    "AAPL": Symbol("alpaca", "Apple"),
    "MSFT": Symbol("alpaca", "Microsoft"),
    "NVDA": Symbol("alpaca", "NVIDIA"),
    "SPY": Symbol("alpaca", "SPDR S&P 500 ETF"),
    "BTCUSDT": Symbol("binance", "Bitcoin on Binance"),
    "BTC-USD": Symbol("coinbase", "Bitcoin on Coinbase"),
}

LABELS = {
    "close": "Close",
    "return_1d": "Daily log return",
    "volatility_20d": "20-day volatility of daily returns",
    "dollar_volume_20d": "20-day average dollar volume",
    "cpi": "CPI index",
    "cpi_yoy": "CPI inflation, year over year",
    "unrate": "Unemployment rate",
    "fedfunds": "Fed funds rate",
    "eps_diluted": "Diluted EPS",
    "operating_cash_flow": "Operating cash flow",
}
SUFFIXES = {"_q": "quarter", "_fy": "fiscal year"}
FRACTIONS = {"return_1d", "volatility_20d", "cpi_yoy"}
PERCENTS = {"unrate", "fedfunds"}
PLAIN = {"close", "cpi", "eps_diluted"}


def table(catalog: str, name: str) -> str:
    if name not in READS:
        raise ValueError(f"{name} is not among the Gold tables the digest may read")
    schema, table_name = name.split(".")
    return f"`{console.validate_catalog(catalog)}`.`{schema}`.`{table_name}`"


def symbol(name: str) -> Symbol:
    if name not in SYMBOLS:
        raise ValueError(f"No digest for {name!r}; choose one of {sorted(SYMBOLS)}.")
    return SYMBOLS[name]


def parse_as_of(text: str) -> datetime:
    try:
        found = datetime.fromisoformat(text.strip())
    except ValueError as error:
        raise ValueError(f"{text!r} is not a date or a date and time.") from error
    if found.tzinfo is None:
        return found.replace(tzinfo=UTC)
    return found.astimezone(UTC)


def as_of_parameter(as_of: datetime) -> str:
    return as_of.strftime("%Y-%m-%d %H:%M:%S")


def ranges_query(catalog: str) -> str:
    return (
        "SELECT venue, symbol, min(session_date) AS first_session, "
        f"max(session_date) AS last_session FROM {table(catalog, 'gold.bars_1d')} "
        "GROUP BY ALL ORDER BY venue, symbol"
    )


def bars_query(catalog: str) -> str:
    return (
        "SELECT session_date, close, volume, knowable_at, knowable_at <= :as_of AS known "
        f"FROM {table(catalog, 'gold.bars_1d')} WHERE venue = :venue AND symbol = :symbol "
        "AND session_date BETWEEN date_sub(to_date(:as_of), :lookback) "
        "AND date_add(to_date(:as_of), :ahead) ORDER BY session_date"
    )


def snapshot_query(catalog: str) -> str:
    return (
        "SELECT entity, feature_group, feature, event_time, value, knowable_at, source_ref "
        f"FROM {table(catalog, 'gold.features')} "
        "WHERE entity IN (:symbol, 'US') AND knowable_at <= :as_of "
        "QUALIFY row_number() OVER (PARTITION BY entity, feature "
        "ORDER BY event_time DESC, knowable_at DESC) = 1 "
        "ORDER BY entity = 'US', feature_group, feature"
    )


def filings_query(catalog: str) -> str:
    return (
        "WITH filings AS (SELECT form, accession_number, "
        "max(period_end) FILTER (WHERE metric <> 'shares_outstanding') AS period_end, "
        f"min(knowable_at) AS knowable_at FROM {table(catalog, 'gold.fundamentals')} "
        "WHERE symbol = :symbol GROUP BY form, accession_number), "
        "seen AS (SELECT *, true AS known FROM filings WHERE knowable_at <= :as_of "
        f"ORDER BY knowable_at DESC LIMIT {KNOWN_FILINGS}), "
        "upcoming AS (SELECT *, false AS known FROM filings WHERE knowable_at > :as_of "
        "ORDER BY knowable_at LIMIT 1) "
        "SELECT * FROM seen UNION ALL SELECT * FROM upcoming ORDER BY knowable_at"
    )


def instant(value: str) -> datetime:
    return parse_as_of(value.replace("Z", "+00:00"))


def label(feature: str) -> str:
    for suffix, duration in SUFFIXES.items():
        if feature.endswith(suffix):
            base = feature.removesuffix(suffix)
            return f"{LABELS.get(base, base.replace('_', ' ').capitalize())}, {duration}"
    return LABELS.get(feature, feature.replace("_", " ").capitalize())


def shown(feature: str, value: float | None) -> str:
    if value is None:
        return "—"
    base = feature.removesuffix("_q").removesuffix("_fy")
    if feature in FRACTIONS:
        return f"{value * 100:.2f}%"
    if feature in PERCENTS:
        return f"{value:.2f}%"
    if base in PLAIN:
        return f"{value:,.2f}"
    if base == "shares_outstanding":
        return f"{value / 1e6:,.0f}M shares"
    return f"${value / 1e6:,.0f}M"


def moves(bars: list[dict]) -> dict:
    known = [b for b in bars if b["known"] and b["close"] is not None]
    if not known:
        return {"last_session": None, "last_close": None, "changes": [], "high": None, "low": None}
    closes = [b["close"] for b in known]
    return {
        "last_session": known[-1]["session_date"],
        "last_close": closes[-1],
        "changes": [
            {"sessions": n, "change": closes[-1] / closes[-1 - n] - 1}
            for n in MOVES
            if len(closes) > n and closes[-1 - n]
        ],
        "high": max(closes),
        "low": min(closes),
    }


def snapshot_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "group": r["feature_group"],
            "feature": r["feature"],
            "label": label(r["feature"]),
            "period": str(r["event_time"])[:10],
            "shown": shown(r["feature"], r["value"]),
            "knowable_at": r["knowable_at"],
            "source": r["source_ref"],
        }
        for r in rows
    ]


def facts(digest: dict) -> list[str]:
    as_of = instant(digest["as_of"])
    lines = []
    found = digest["moves"]
    if found["last_close"] is not None:
        lines.append(f"Last close {found['last_close']:,.2f} on {found['last_session']}.")
        for move in found["changes"]:
            sessions = "1 session" if move["sessions"] == 1 else f"{move['sessions']} sessions"
            lines.append(f"Change over {sessions}: {move['change'] * 100:+.2f}%.")
        low, high = found["low"], found["high"]
        lines.append(f"Range over the last {LOOKBACK_DAYS} days: {low:,.2f} to {high:,.2f}.")
    for row in digest["snapshot"]:
        if instant(row["knowable_at"]) > as_of:
            raise LeakageFound(f"{row['feature']} became public at {row['knowable_at']}")
        if row["group"] == "prices":
            continue
        lines.append(
            f"{row['label']} for {row['period']}: {row['shown']} "
            f"(public since {row['knowable_at'][:16]} UTC)."
        )
    for filing in digest["filings"]:
        if not filing["known"]:
            continue
        if instant(filing["knowable_at"]) > as_of:
            raise LeakageFound(f"filing {filing['accession_number']} is after {digest['as_of']}")
        lines.append(
            f"Filed a {filing['form']} for the period ending {filing['period_end']} "
            f"on {filing['knowable_at'][:10]}."
        )
    return lines


def prompt(digest: dict) -> list[dict]:
    as_of = digest["as_of"][:16].replace("T", " ")
    system = (
        f"You write a short digest of {digest['name']} ({digest['symbol']}) as it could have "
        f"been written at {as_of} UTC. Use only the facts you are given; each one was public "
        f"by then. Never mention or guess anything after {as_of} UTC. Do not predict prices and "
        "do not recommend buying or selling. Write three to five sentences of plain prose: the "
        "recent price move first, then the latest reported results if there are any, then the "
        "macro backdrop, and name the filing a reported figure comes from."
    )
    known = "\n".join(f"- {line}" for line in facts(digest))
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Facts known at {as_of} UTC:\n{known}"},
    ]

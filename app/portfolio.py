import math
from datetime import date, timedelta

import digest

MAX_POSITIONS = len(digest.SYMBOLS)
MAX_QUANTITY = 1e9
PERIODS = {"1D": 1, "1W": 7, "1M": 30, "3M": 90}
KEY_RESULTS = ("revenue_q", "net_income_q", "eps_diluted_q")
MACRO = ("cpi_yoy", "unrate", "fedfunds")


def holdings(rows: list[tuple[str, float]]) -> dict[str, float]:
    if not rows:
        raise ValueError("Add at least one position.")
    found: dict[str, float] = {}
    for symbol, quantity in rows:
        digest.symbol(symbol)
        if not math.isfinite(quantity) or quantity <= 0 or quantity > MAX_QUANTITY:
            raise ValueError(f"The quantity of {symbol} must be above 0 and at most 1e9.")
        found[symbol] = found.get(symbol, 0.0) + quantity
    if len(found) > MAX_POSITIONS:
        raise ValueError(f"At most {MAX_POSITIONS} positions.")
    return found


def value_series(
    bars: dict[str, list[dict]], quantities: dict[str, float], known_only: bool
) -> list[tuple[str, float]]:
    rows = {
        symbol: {
            b["session_date"]: b["close"]
            for b in found
            if b["close"] is not None and (b["known"] or not known_only)
        }
        for symbol, found in bars.items()
    }
    priced = [s for s in quantities if rows.get(s)]
    if not priced:
        return []
    closes: dict[str, float] = {}
    series = []
    for day in sorted(set().union(*(rows[s].keys() for s in priced))):
        for symbol in priced:
            if day in rows[symbol]:
                closes[symbol] = rows[symbol][day]
        if len(closes) == len(priced):
            series.append((day, sum(quantities[s] * closes[s] for s in priced)))
    return series


def changes(series: list[tuple[str, float]]) -> list[dict]:
    if not series:
        return []
    last_day, last_value = series[-1]
    found = []
    for label, days in PERIODS.items():
        target = (date.fromisoformat(last_day) - timedelta(days=days)).isoformat()
        earlier = [v for d, v in series if d <= target]
        if earlier and earlier[-1]:
            found.append({"period": label, "change": last_value / earlier[-1] - 1})
    return found


def positions(digests: dict[str, dict], quantities: dict[str, float]) -> list[dict]:
    rows = []
    for symbol, quantity in quantities.items():
        moves = digests[symbol]["moves"]
        close = moves["last_close"]
        rows.append(
            {
                "symbol": symbol,
                "name": digests[symbol]["name"],
                "quantity": quantity,
                "last_session": moves["last_session"],
                "last_close": close,
                "value": None if close is None else quantity * close,
                "changes": moves["changes"],
            }
        )
    total = sum(r["value"] for r in rows if r["value"] is not None)
    for row in rows:
        row["weight"] = row["value"] / total if row["value"] is not None and total else None
    return rows


def facts(summary: dict, digests: dict[str, dict]) -> list[str]:
    for found in digests.values():
        digest.facts(found)
    lines = []
    if summary["value"] is not None:
        lines.append(
            f"Portfolio value {summary['value']:,.2f} USD from the closes known on "
            f"{summary['last_day']}."
        )
    for change in summary["changes"]:
        lines.append(f"Portfolio change over {change['period']}: {change['change'] * 100:+.2f}%.")
    for row in summary["positions"]:
        if row["value"] is None:
            lines.append(f"{row['symbol']}: no close was known at the instant.")
            continue
        moved = ", ".join(
            f"{m['sessions']} session{'s' if m['sessions'] > 1 else ''} {m['change'] * 100:+.2f}%"
            for m in row["changes"]
        )
        lines.append(
            f"{row['symbol']} ({row['name']}): {row['quantity']:g} at {row['last_close']:,.2f} "
            f"on {row['last_session']} = {row['value']:,.2f} USD, {row['weight'] * 100:.1f}% "
            f"of the portfolio" + (f"; moves: {moved}." if moved else ".")
        )
    macro: dict[str, dict] = {}
    for symbol, found in digests.items():
        for value in found["snapshot"]:
            if value["feature"] in KEY_RESULTS:
                lines.append(
                    f"{symbol} {value['label']} for {value['period']}: {value['shown']} "
                    f"(public since {value['knowable_at'][:16]} UTC)."
                )
            if value["feature"] in MACRO:
                macro[value["feature"]] = value
        filed = [f for f in found["filings"] if f["known"]]
        if filed:
            latest = filed[-1]
            lines.append(
                f"{symbol} last filed a {latest['form']} for the period ending "
                f"{latest['period_end']} on {latest['knowable_at'][:10]}."
            )
    for value in macro.values():
        lines.append(
            f"{value['label']} for {value['period']}: {value['shown']} "
            f"(public since {value['knowable_at'][:16]} UTC)."
        )
    return lines


def prompt(summary: dict, digests: dict[str, dict]) -> list[dict]:
    as_of = summary["as_of"][:16].replace("T", " ")
    system = (
        f"You write a short digest of an investment portfolio as it could have been written at "
        f"{as_of} UTC. Use only the facts you are given; each one was public by then. Never "
        f"mention or guess anything after {as_of} UTC. Do not predict prices, do not recommend "
        "buying, selling or rebalancing. Write four to six sentences of plain prose: how the "
        "portfolio's value moved and which positions drove it, then the latest reported results "
        "of its companies, then the macro backdrop."
    )
    known = "\n".join(f"- {line}" for line in facts(summary, digests))
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Facts known at {as_of} UTC:\n{known}"},
    ]

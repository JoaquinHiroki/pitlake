import pytest

import digest
import portfolio


def bar(day, close, known=True):
    return {"session_date": day, "close": close, "known": known}


BARS = {
    "AAPL": [bar("2025-01-03", 100.0), bar("2025-01-06", 110.0), bar("2025-01-07", 120.0, False)],
    "BTCUSDT": [
        bar("2025-01-04", 1000.0),
        bar("2025-01-05", 1100.0),
        bar("2025-01-06", 1200.0),
        bar("2025-01-07", 900.0, False),
    ],
}


def test_holdings_are_checked_and_merged():
    assert portfolio.holdings([("AAPL", 2), ("SPY", 1), ("AAPL", 3)]) == {"AAPL": 5, "SPY": 1}
    for rows, message in [
        ([], "at least one"),
        ([("TSLA", 1)], "No digest"),
        ([("AAPL", 0)], "above 0"),
        ([("AAPL", float("nan"))], "above 0"),
        ([("AAPL", 2e9)], "at most"),
    ]:
        with pytest.raises(ValueError, match=message):
            portfolio.holdings(rows)


def test_the_value_starts_when_every_priced_position_has_a_close_and_carries_it_forward():
    quantities = {"AAPL": 10, "BTCUSDT": 1, "NVDA": 5}
    known = portfolio.value_series(BARS, quantities, known_only=True)
    assert known == [
        ("2025-01-04", 10 * 100 + 1000),
        ("2025-01-05", 10 * 100 + 1100),
        ("2025-01-06", 10 * 110 + 1200),
    ]
    full = portfolio.value_series(BARS, quantities, known_only=False)
    assert full[-1] == ("2025-01-07", 10 * 120 + 900)
    assert portfolio.value_series({"NVDA": []}, {"NVDA": 1}, known_only=True) == []


def test_changes_compare_with_the_value_on_or_before_each_period():
    series = [("2025-01-01", 100.0), ("2025-01-07", 110.0), ("2025-01-08", 121.0)]
    found = {c["period"]: c["change"] for c in portfolio.changes(series)}
    assert found["1D"] == pytest.approx(0.1)
    assert found["1W"] == pytest.approx(0.21)
    assert "1M" not in found and portfolio.changes([]) == []


def holding_digest(symbol, close, known_at="2024-11-01T10:01:36.000Z"):
    return {
        "symbol": symbol,
        "name": digest.SYMBOLS[symbol].name,
        "as_of": "2025-01-08T00:00:00Z",
        "moves": {
            "last_session": None if close is None else "2025-01-06",
            "last_close": close,
            "changes": [] if close is None else [{"sessions": 1, "change": 0.01}],
            "high": close,
            "low": close,
        },
        "snapshot": digest.snapshot_rows(
            [
                {
                    "feature_group": "fundamentals",
                    "feature": "revenue_q",
                    "event_time": "2024-06-29",
                    "value": 85_777_000_000.0,
                    "knowable_at": known_at,
                    "source_ref": "a1",
                },
                {
                    "feature_group": "macro",
                    "feature": "unrate",
                    "event_time": "2024-12-01",
                    "value": 4.2,
                    "knowable_at": "2025-01-03T13:30:00.000Z",
                    "source_ref": "2025-01-03",
                },
            ]
        ),
        "filings": [
            {
                "form": "10-K",
                "accession_number": "a1",
                "period_end": "2024-09-28",
                "knowable_at": "2024-11-01T10:01:36.000Z",
                "known": True,
            }
        ],
    }


def test_positions_weigh_by_known_value_and_the_prompt_uses_only_known_facts():
    digests = {"AAPL": holding_digest("AAPL", 300.0), "NVDA": holding_digest("NVDA", None)}
    rows = portfolio.positions(digests, {"AAPL": 10, "NVDA": 5})
    assert rows[0]["value"] == 3000 and rows[0]["weight"] == 1
    assert rows[1]["value"] is None and rows[1]["weight"] is None
    summary = {
        "as_of": "2025-01-08T00:00:00Z",
        "value": 3000.0,
        "last_day": "2025-01-06",
        "changes": [{"period": "1D", "change": 0.01}],
        "positions": rows,
    }
    messages = portfolio.prompt(summary, digests)
    assert "do not recommend buying, selling or rebalancing" in messages[0]["content"]
    facts = messages[1]["content"]
    assert "Portfolio value 3,000.00 USD" in facts
    assert "AAPL (Apple): 10 at 300.00 on 2025-01-06 = 3,000.00 USD, 100.0%" in facts
    assert "NVDA: no close was known at the instant." in facts
    assert "AAPL Revenue, quarter for 2024-06-29: $85,777M" in facts
    assert "AAPL last filed a 10-K for the period ending 2024-09-28" in facts
    assert facts.count("Unemployment rate") == 1


def test_a_holding_with_a_later_value_stops_the_portfolio_digest():
    digests = {"AAPL": holding_digest("AAPL", 300.0, known_at="2025-01-09T00:00:00.000Z")}
    rows = portfolio.positions(digests, {"AAPL": 1})
    summary = {"as_of": "2025-01-08T00:00:00Z", "value": 300.0, "last_day": "2025-01-06"}
    with pytest.raises(digest.LeakageFound):
        portfolio.prompt(summary | {"changes": [], "positions": rows}, digests)

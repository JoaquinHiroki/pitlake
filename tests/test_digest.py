import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

import digest

ROOT = Path(__file__).resolve().parents[1]
QUERIES = (digest.ranges_query, digest.bars_query, digest.snapshot_query, digest.filings_query)
AS_OF = "2025-08-02T00:00:00Z"


def bar(day, close, known=True):
    return {"session_date": day, "close": close, "known": known}


def known_digest(**changes):
    found = {
        "symbol": "AAPL",
        "name": "Apple",
        "as_of": AS_OF,
        "moves": digest.moves([bar("2025-07-31", 200.0), bar("2025-08-01", 202.0)]),
        "snapshot": digest.snapshot_rows(
            [
                {
                    "feature_group": "fundamentals",
                    "feature": "revenue_q",
                    "event_time": "2025-06-28T00:00:00.000Z",
                    "value": 94_036_000_000.0,
                    "knowable_at": "2025-08-01T10:30:00.000Z",
                    "source_ref": "0000320193-25-000073",
                }
            ]
        ),
        "filings": [
            {
                "form": "10-Q",
                "accession_number": "0000320193-25-000073",
                "period_end": "2025-06-28",
                "knowable_at": "2025-08-01T10:30:00.000Z",
                "known": True,
            },
            {
                "form": "10-K",
                "accession_number": "0000320193-25-000079",
                "period_end": "2025-09-27",
                "knowable_at": "2025-10-31T10:01:00.000Z",
                "known": False,
            },
        ],
    }
    return found | changes


def test_the_digest_reads_only_gold_tables_the_app_is_granted():
    bundle = (ROOT / "resources" / "console.yml").read_text()
    granted = set(re.findall(r"securable_full_name: \$\{var\.catalog\}\.(\S+)", bundle))
    assert set(digest.READS) <= granted
    read = set()
    for build in QUERIES:
        read |= set(re.findall(r"`pitlake_prod`\.`(\w+)`\.`(\w+)`", build("pitlake_prod")))
    assert sorted(f"{s}.{t}" for s, t in read) == sorted(digest.READS)
    with pytest.raises(ValueError, match="may read"):
        digest.table("pitlake_prod", "silver.edgar_filings")


def test_the_digest_keeps_only_values_public_at_the_instant():
    snapshot = digest.snapshot_query("pitlake_dev")
    assert "knowable_at <= :as_of" in snapshot
    assert "ORDER BY event_time DESC, knowable_at DESC) = 1" in snapshot
    assert "knowable_at <= :as_of AS known" in digest.bars_query("pitlake_dev")
    filings = digest.filings_query("pitlake_dev")
    assert "knowable_at <= :as_of" in filings and "knowable_at > :as_of" in filings
    assert "FILTER (WHERE metric <> 'shares_outstanding')" in filings


def test_an_instant_without_a_zone_is_utc_and_a_date_is_midnight():
    assert digest.parse_as_of("2025-08-02") == datetime(2025, 8, 2, tzinfo=UTC)
    assert digest.parse_as_of("2025-08-01T22:30") == datetime(2025, 8, 1, 22, 30, tzinfo=UTC)
    assert digest.parse_as_of("2025-08-02T00:30+02:00") == datetime(2025, 8, 1, 22, 30, tzinfo=UTC)
    assert digest.as_of_parameter(digest.parse_as_of("2025-08-02")) == "2025-08-02 00:00:00"
    with pytest.raises(ValueError, match="not a date"):
        digest.parse_as_of("yesterday")
    with pytest.raises(ValueError, match="No digest"):
        digest.symbol("TSLA")


def test_moves_use_only_the_known_sessions():
    bars = [bar(f"2025-07-{d:02}", 100.0 + d) for d in range(1, 31)]
    bars.append(bar("2025-07-31", 500.0, known=False))
    found = digest.moves(bars)
    assert found["last_session"] == "2025-07-30" and found["last_close"] == 130.0
    assert [m["sessions"] for m in found["changes"]] == [1, 5, 20]
    assert found["changes"][0]["change"] == pytest.approx(130 / 129 - 1)
    assert found["high"] == 130.0 and found["low"] == 101.0
    assert digest.moves([bar("2025-07-31", 500.0, known=False)])["last_close"] is None


def test_values_are_labelled_and_shown_in_their_units():
    assert digest.label("revenue_q") == "Revenue, quarter"
    assert digest.label("eps_diluted_fy") == "Diluted EPS, fiscal year"
    assert digest.label("cpi_yoy") == "CPI inflation, year over year"
    assert digest.shown("revenue_q", 94_036_000_000.0) == "$94,036M"
    assert digest.shown("eps_diluted_q", 1.57) == "1.57"
    assert digest.shown("cpi_yoy", 0.027) == "2.70%"
    assert digest.shown("unrate", 4.2) == "4.20%"
    assert digest.shown("shares_outstanding", 14_840_000_000.0) == "14,840M shares"
    assert digest.shown("close", None) == "—"


def test_the_prompt_gets_the_known_facts_and_never_the_next_filing():
    messages = digest.prompt(known_digest())
    assert [m["role"] for m in messages] == ["system", "user"]
    assert "2025-08-02 00:00 UTC" in messages[0]["content"]
    assert "do not recommend buying or selling" in messages[0]["content"]
    facts = messages[1]["content"]
    assert "Revenue, quarter for 2025-06-28: $94,036M" in facts
    assert "Filed a 10-Q for the period ending 2025-06-28 on 2025-08-01" in facts
    assert "Change over 1 session: +1.00%" in facts
    assert "10-K" not in facts


def test_a_value_public_after_the_instant_stops_the_digest():
    leaked = known_digest()
    leaked["snapshot"][0]["knowable_at"] = "2025-08-02T00:00:01.000Z"
    with pytest.raises(digest.LeakageFound, match="revenue_q"):
        digest.facts(leaked)
    leaked = known_digest()
    leaked["filings"][1]["known"] = True
    with pytest.raises(digest.LeakageFound, match="000079"):
        digest.facts(leaked)

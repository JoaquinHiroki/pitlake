import math
import os
from datetime import date, timedelta
from decimal import Decimal

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from world import ny_end, utc  # noqa: E402

from pitlake.gold import (  # noqa: E402
    BAR_COLUMNS,
    DAILY_COLUMNS,
    FEATURE_COLUMNS,
    WINDOW_DAYS,
    build_features,
    candle_minute_bars,
    daily_bars,
    fundamental_features,
    fundamentals,
    macro_features,
    price_features,
    trade_minute_bars,
)
from pitlake.quality import FUNDAMENTALS_DATED_BY_PERIOD  # noqa: E402

DAY = date(2025, 1, 2)
TRADES = (
    "symbol STRING, trade_id BIGINT, price DECIMAL(28,10), quantity DECIMAL(28,10), "
    "event_time TIMESTAMP, knowable_at TIMESTAMP, _period_start DATE, _source_file STRING, "
    "_source_sha256 STRING"
)


def at(minutes, seconds=0):
    return utc(2025, 1, 2) + timedelta(minutes=minutes, seconds=seconds)


def trade(trade_id, price, quantity, when):
    return ("BTCUSDT", trade_id, Decimal(price), Decimal(quantity), when, when, DAY, "f1", "s1")


@pytest.fixture(scope="module")
def minute(spark):
    trades = spark.createDataFrame(
        [
            trade(2, "101", "2", at(0, 50)),
            trade(1, "100", "1", at(0, 10)),
            trade(3, "99", "1", at(1)),
            trade(4, "102", "1", at(1)),
        ],
        TRADES,
    )
    return {r["bar_time"]: r for r in trade_minute_bars(trades, "binance").collect()}


def test_trades_become_minute_bars_known_when_the_minute_ends(minute):
    first, second = minute[at(0)], minute[at(1)]
    assert (first["open"], first["high"], first["low"], first["close"]) == (
        Decimal("100"),
        Decimal("101"),
        Decimal("100"),
        Decimal("101"),
    )
    assert (first["volume"], first["trades"]) == (Decimal("3"), 2)
    assert float(first["vwap"]) == pytest.approx(302 / 3)
    assert first["knowable_at"] == at(1)
    assert (second["open"], second["close"], second["knowable_at"]) == (
        Decimal("99"),
        Decimal("102"),
        at(2),
    )
    assert tuple(first.asDict()) == BAR_COLUMNS
    assert first["venue"] == "binance" and first["period"] == DAY


def test_candles_pass_through_as_bars(spark):
    candles = spark.createDataFrame(
        [("BTC-USD", *(Decimal(v) for v in ("1", "2", "0.5", "1.5", "7")), at(5), at(6), DAY)],
        "symbol STRING, open DECIMAL(28,10), high DECIMAL(28,10), low DECIMAL(28,10), "
        "close DECIMAL(28,10), volume DECIMAL(28,10), event_time TIMESTAMP, "
        "knowable_at TIMESTAMP, _period_start DATE",
    ).selectExpr("*", "'f2' AS _source_file", "'s2' AS _source_sha256")
    (bar,) = candle_minute_bars(candles, "coinbase").collect()
    assert (bar["bar_time"], bar["knowable_at"], bar["close"]) == (at(5), at(6), Decimal("1.5"))
    assert bar["trades"] is None and bar["vwap"] is None


def test_daily_bars_from_minutes_and_from_consolidated_bars(spark, minute):
    bars = spark.createDataFrame(list(minute.values()), trade_minute_bars_schema(spark))
    equities = spark.createDataFrame(
        [
            (
                "AAPL",
                DAY,
                *(Decimal(v) for v in ("1", "2", "1", "2", "5")),
                3,
                Decimal(2),
                ny_end(DAY),
            )
        ],
        "symbol STRING, session_date DATE, open DECIMAL(28,10), high DECIMAL(28,10), "
        "low DECIMAL(28,10), close DECIMAL(28,10), volume DECIMAL(28,10), trade_count BIGINT, "
        "vwap DECIMAL(28,10), knowable_at TIMESTAMP",
    )
    days = {r["symbol"]: r for r in daily_bars(bars, equities).collect()}
    btc = days["BTCUSDT"]
    assert (btc["open"], btc["high"], btc["low"], btc["close"]) == (
        Decimal("100"),
        Decimal("102"),
        Decimal("99"),
        Decimal("102"),
    )
    assert (btc["volume"], btc["trades"], btc["minutes"]) == (Decimal("5"), 4, 2)
    assert btc["knowable_at"] == utc(2025, 1, 3)
    assert float(btc["vwap"]) == pytest.approx((302 + 99 + 102) / 5)
    assert days["AAPL"]["knowable_at"] == ny_end(DAY)
    assert days["AAPL"]["venue"] == "alpaca" and days["AAPL"]["minutes"] is None
    assert tuple(btc.asDict()) == DAILY_COLUMNS


def trade_minute_bars_schema(spark):
    empty = spark.createDataFrame([], TRADES)
    return trade_minute_bars(empty, "binance").schema


def test_fundamentals_keep_every_filing_and_choose_one_concept(spark, world):
    rows = fundamentals(spark, world["facts"]).collect()
    q1 = sorted(
        (r["accession_number"], r["concept"], float(r["value"]))
        for r in rows
        if r["metric"] == "revenue" and r["period_end"] == date(2024, 12, 28)
    )
    assert q1 == [
        ("0000320193-25-000008", "Revenues", 124.3),
        ("0000320193-25-000020", "Revenues", 125.0),
        ("0000320193-25-000099", "Revenues", 999999.0),
    ]
    durations = {(r["metric"], r["duration"], r["period_end"]) for r in rows}
    assert ("revenue", "annual", date(2024, 9, 28)) in durations
    assert ("assets", "instant", date(2025, 3, 29)) in durations
    ytd = (date(2024, 9, 29), date(2025, 3, 29))
    assert not any((r["period_start"], r["period_end"]) == ytd for r in rows)


def test_fundamental_features_are_known_at_acceptance_not_period_end(spark, world):
    table = fundamentals(spark, world["facts"])
    honest = {
        (r["entity"], r["feature"], r["source_ref"]): r
        for r in fundamental_features(table).collect()
    }
    q2 = honest[("AAPL", "revenue_q", "0000320193-25-000057")]
    assert q2["event_time"] == utc(2025, 3, 29)
    assert q2["knowable_at"] == utc(2025, 5, 2, 20, 30)
    assert honest[("AAPL", "revenue_fy", "0000320193-24-000123")]["knowable_at"] == ny_end(
        date(2024, 11, 1)
    )
    biased = fundamental_features(table, FUNDAMENTALS_DATED_BY_PERIOD).where(
        "source_ref = '0000320193-25-000057' AND feature = 'revenue_q'"
    )
    assert biased.collect()[0]["knowable_at"] == utc(2025, 3, 29)


def test_macro_features_keep_every_vintage_and_compare_years_within_one(world):
    rows = macro_features(world["vintages"]).collect()
    assert {r["entity"] for r in rows} == {"US"}
    unrate = {
        (r["event_time"], r["source_ref"]): r["value"] for r in rows if r["feature"] == "unrate"
    }
    assert unrate[(utc(2024, 12, 1), "2025-01-10")] == pytest.approx(4.1)
    assert unrate[(utc(2024, 12, 1), "2025-02-07")] == pytest.approx(4.15)
    yoy = {(r["event_time"], r["source_ref"]): r for r in rows if r["feature"] == "cpi_yoy"}
    assert yoy[(utc(2024, 12, 1), "2025-01-15")]["value"] == pytest.approx(0.03)
    assert yoy[(utc(2024, 12, 1), "2025-02-12")]["value"] == pytest.approx(0.035)
    assert yoy[(utc(2025, 1, 1), "2025-02-12")]["value"] == pytest.approx(310 / 301 - 1)
    assert yoy[(utc(2025, 1, 1), "2025-02-12")]["knowable_at"] == ny_end(date(2025, 2, 12))
    assert len(yoy) == 3


def test_price_features_need_a_full_window(world):
    rows = price_features(world["bars_1d"].where("symbol = 'AAPL'")).collect()
    by = {(r["feature"], r["event_time"].date()): r for r in rows}
    days = world["days"]
    assert by[("return_1d", days[1])]["value"] == pytest.approx(math.log(102 / 100))
    assert ("return_1d", days[0]) not in by
    assert ("volatility_20d", days[WINDOW_DAYS - 1]) not in by
    assert ("volatility_20d", days[WINDOW_DAYS]) in by
    assert ("dollar_volume_20d", days[WINDOW_DAYS]) in by
    close = by[("close", days[3])]
    assert (close["value"], close["knowable_at"], close["source_ref"]) == (
        103.0,
        ny_end(days[3]),
        days[3].isoformat(),
    )


def test_every_feature_row_has_the_feature_table_columns(spark, world):
    features = build_features(spark, world["bars_1d"], world["facts"], world["vintages"])
    assert tuple(features.columns) == FEATURE_COLUMNS
    assert features.where("knowable_at IS NULL OR source_ref IS NULL").count() == 0
    assert features.where("knowable_at < event_time").count() == 0

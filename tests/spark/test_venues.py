import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.gate import BTC, THRESHOLDS, judge, venue_measurements  # noqa: E402
from pitlake.quality import NO_FAULT, VENUE_MISSING_MINUTES, VENUE_PRICE_DIVERGENCE  # noqa: E402
from pitlake.venues import (  # noqa: E402
    COMPARISON_VALUES,
    compare,
    comparison_rows,
    daily_counts,
    minute_bars,
    minute_candles,
)

DAY = date(2025, 1, 2)
START = datetime(2025, 1, 2, tzinfo=UTC)


def at(minutes, seconds=0):
    return START + timedelta(minutes=minutes, seconds=seconds)


@pytest.fixture(scope="module")
def venues(spark):
    trades = spark.createDataFrame(
        [
            ("BTCUSDT", 1, Decimal("100"), Decimal("1"), at(0, 10), DAY),
            ("BTCUSDT", 2, Decimal("101"), Decimal("2"), at(0, 50), DAY),
            ("BTCUSDT", 3, Decimal("100"), Decimal("1"), at(1), DAY),
            ("BTCUSDT", 4, Decimal("100"), Decimal("1"), at(2), DAY),
            ("BTCUSDT", 5, Decimal("100"), Decimal("1"), at(720), DAY),
            ("ETHUSDT", 6, Decimal("5"), Decimal("1"), at(3), DAY),
        ],
        "symbol STRING, trade_id BIGINT, price DECIMAL(28,10), quantity DECIMAL(28,10), "
        "event_time TIMESTAMP, _period_start DATE",
    )
    candles = spark.createDataFrame(
        [
            ("BTC-USD", Decimal("100"), Decimal("101"), Decimal("100"), Decimal("101"), at(0)),
            ("BTC-USD", Decimal("100"), Decimal("102"), Decimal("100"), Decimal("102"), at(1)),
            ("BTC-USD", Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), at(3)),
            ("BTC-USD", Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), at(720)),
        ],
        "symbol STRING, open DECIMAL(28,10), high DECIMAL(28,10), low DECIMAL(28,10), "
        "close DECIMAL(28,10), event_time TIMESTAMP",
    ).selectExpr(
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "CAST(3 AS DECIMAL(28,10)) AS volume",
        "event_time",
        f"DATE'{DAY}' AS _period_start",
    )
    left = minute_bars(trades, "BTCUSDT", [DAY])
    right = minute_candles(candles, "BTC-USD", [DAY])
    return left, right


def statuses(compared) -> dict:
    return {
        r["m"]: r["status"]
        for r in compared.selectExpr(
            "CAST((unix_seconds(event_time) - unix_seconds(TIMESTAMP'2025-01-02 00:00:00')) / 60 "
            "AS INT) AS m",
            "status",
        )
        .where("status <> 'missing_both'")
        .collect()
    }


def test_every_minute_of_the_day_is_judged(spark, venues):
    compared = compare(spark, BTC, *venues, [DAY])
    assert compared.count() == 1440
    assert statuses(compared) == {
        0: "agree",
        1: "price_divergence",
        2: "missing_right",
        3: "missing_left",
        720: "agree",
    }
    first = compared.where("left_trades = 2").first()
    assert (first["left_open"], first["left_close"], first["left_volume"]) == (
        Decimal("100"),
        Decimal("101"),
        Decimal("3"),
    )
    assert first["close_diff_bps"] == 0
    assert compared.selectExpr("min(knowable_at > event_time)").first()[0] is True


def test_daily_counts_feed_the_gate(spark, venues):
    (row,) = daily_counts(compare(spark, BTC, *venues, [DAY])).collect()
    assert (row["minutes"], row["divergent"], row["missing_left"], row["missing_right"]) == (
        1440,
        1,
        1,
        1,
    )
    results = judge(venue_measurements(BTC, [(DAY, 1440, row["divergent"], 2)]), THRESHOLDS, {})
    assert not any(r.breach for r in results)


@pytest.mark.parametrize(
    ("fault", "status"),
    [(VENUE_PRICE_DIVERGENCE, "price_divergence"), (VENUE_MISSING_MINUTES, "missing_right")],
)
def test_an_injected_discrepancy_breaks_the_noon_hour(spark, venues, fault, status):
    assert statuses(compare(spark, BTC, *venues, [DAY], NO_FAULT))[720] == "agree"
    assert statuses(compare(spark, BTC, *venues, [DAY], fault))[720] == status


def test_rows_written_match_the_table(spark, venues):
    class Files:
        left_file, right_file = "binance/f", "coinbase/f"

    rows = comparison_rows(spark, compare(spark, BTC, *venues, [DAY]), {DAY: Files}, "1", START)
    assert rows.columns == [
        "pair",
        "day",
        "event_time",
        "knowable_at",
        "status",
        "close_diff_bps",
        *(f"{side}_{c}" for side, c, _ in COMPARISON_VALUES),
        "_left_file",
        "_right_file",
        "_compared_at",
        "_run_id",
    ]
    types = dict(rows.dtypes)
    assert all(types[f"{side}_{c}"] == kind for side, c, kind in COMPARISON_VALUES)
    assert rows.where("_left_file = 'binance/f'").count() == 1440

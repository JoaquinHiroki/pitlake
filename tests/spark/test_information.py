import os
from datetime import date

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.backtest import decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.information import (  # noqa: E402
    WORLD_SCHEMA,
    account,
    enumerate_items,
    feature_rows,
    in_reach,
    information_set,
    sessions,
)
from pitlake.information import world as world_of  # noqa: E402
from pitlake.jobs.information_set import REACH  # noqa: E402
from pitlake.jobs.research import fingerprint_inputs, read_inputs  # noqa: E402

TEN_Q = "filing:0000320193-25-000008"
OLDEST_BAR = "bar:BTCUSDT/2025-01-02"


@pytest.fixture(scope="module")
def enumerated(spark, world):
    from pyspark.sql import functions as F

    features = build_features(spark, world["bars_1d"], world["facts"], world["vintages"])
    bars = world["bars_1d"].select("*", *(F.col("close").alias(c) for c in ("open", "high", "low")))
    items = enumerate_items(bars, fundamentals(spark, world["facts"]), features, REACH)
    times = decision_times(date(2025, 2, 1), date(2025, 2, 25))
    _, recorded = fingerprint_inputs(read_inputs(spark, features, times))
    return items, times, recorded, feature_rows(in_reach(features, REACH))


def rebuild(spark, info, rows, times):
    kept = world_of(rows, info)
    received = read_inputs(spark, spark.createDataFrame(kept, WORLD_SCHEMA), times)
    return received, fingerprint_inputs(received)[1]


def test_the_items_alone_rebuild_the_recorded_inputs(spark, enumerated):
    items, times, recorded, rows = enumerated
    info = information_set(items, times[-1])
    received, rebuilt = rebuild(spark, info, rows, times)
    assert rebuilt == recorded
    found = account(info, rows, received, sessions(items))
    assert (found.unaccounted, found.sample) == (0, ())
    assert {TEN_Q, "release:CPIAUCSL/2025-01-15", "release:CPIAUCSL/2025-02-12"} <= found.read
    assert OLDEST_BAR in info.items and OLDEST_BAR not in found.read
    assert not any(i.startswith("release:UNRATE") for i in info.items)
    assert "filing:0000320193-25-000099" not in info.items


def test_leaving_out_a_filing_that_was_read_changes_the_inputs(spark, enumerated):
    items, times, recorded, rows = enumerated
    info = information_set(items, times[-1], [TEN_Q])
    received, rebuilt = rebuild(spark, info, rows, times)
    assert rebuilt != recorded
    assert account(info, rows, received, sessions(items)).unaccounted > 0


def test_leaving_out_an_unread_bar_keeps_the_hash_but_not_the_accounting(spark, enumerated):
    items, times, recorded, rows = enumerated
    info = information_set(items, times[-1], [OLDEST_BAR])
    received, rebuilt = rebuild(spark, info, rows, times)
    assert rebuilt == recorded
    found = account(info, rows, received, sessions(items))
    assert found.unaccounted > 0 and OLDEST_BAR in found.sample[0]

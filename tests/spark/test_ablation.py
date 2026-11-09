import os
from datetime import date

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.ablation import HIDE, rebuild  # noqa: E402
from pitlake.backtest import decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.information import enumerate_items, feature_rows, in_reach  # noqa: E402
from pitlake.jobs.ablation import COHERENT, ablate  # noqa: E402
from pitlake.jobs.information_set import REACH, Loaded, rebuilt_inputs  # noqa: E402
from pitlake.jobs.research import fingerprint_inputs  # noqa: E402

MIDDLE_BAR = "bar:BTCUSDT/2025-01-12"
TEN_Q = "filing:0000320193-25-000008"
FEBRUARY_CPI = "release:CPIAUCSL/2025-02-12"


@pytest.fixture(scope="module")
def loaded(spark, world):
    from pyspark.sql import functions as F

    features = build_features(spark, world["bars_1d"], world["facts"], world["vintages"])
    bars = world["bars_1d"].select("*", *(F.col("close").alias(c) for c in ("open", "high", "low")))
    facts = fundamentals(spark, world["facts"])
    return Loaded(
        {},
        features,
        bars,
        facts,
        enumerate_items(bars, facts, features, REACH),
        decision_times(date(2025, 2, 1), date(2025, 2, 25)),
        feature_rows(in_reach(features, REACH)),
    )


@pytest.fixture(scope="module")
def recorded(spark, loaded):
    return fingerprint_inputs(rebuilt_inputs(spark, loaded.rows, loaded.times))[1]


def test_the_null_world_is_gold(spark, loaded, recorded):
    rebuilt = rebuild(loaded.bars, loaded.fundamentals, loaded.features, (), REACH)
    assert sorted(map(repr, rebuilt)) == sorted(map(repr, loaded.rows))
    null = ablate(spark, loaded, (), COHERENT)
    assert null.coherent(recorded) and null.input_hash == recorded
    assert (len(null.removed), len(null.recomputed)) == (0, 0)


def test_withholding_a_bar_recomputes_the_windows_that_read_it(spark, loaded, recorded):
    found = ablate(spark, loaded, (MIDDLE_BAR,), COHERENT)
    assert found.coherent(recorded), found.sample
    removed = sorted((r[1], r[6]) for r in found.removed)
    closes = [("close", f"2025-01-{d}") for d in range(12, 27)]
    assert removed == closes + [("volatility_20d", f"2025-01-{d}") for d in range(22, 27)]
    recomputed = sorted((r[1], r[6]) for r in found.recomputed)
    assert recomputed == closes[1:] + [("volatility_20d", f"2025-01-{d}") for d in range(23, 27)]
    assert found.input_hash != recorded


def test_hiding_a_bar_without_recomputing_is_caught(spark, loaded, recorded):
    found = ablate(spark, loaded, (MIDDLE_BAR,), HIDE)
    assert not found.coherent(recorded)
    assert (found.extra, found.missing) == (5, 4)
    assert found.owned == 0 and found.accounting.unaccounted == 0


def test_withholding_a_filing_and_a_release_together(spark, loaded, recorded):
    found = ablate(spark, loaded, (TEN_Q, FEBRUARY_CPI), COHERENT)
    assert found.coherent(recorded), found.sample
    assert found.recomputed == () and found.owned == 0
    assert {r[6] for r in found.removed} == {"0000320193-25-000008", "2025-02-12"}
    assert found.input_hash != recorded
    assert TEN_Q not in found.accounting.read

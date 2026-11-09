import os
from datetime import date

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.ablation import apply, plan, recompute  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH, inputs_at, inputs_hash  # noqa: E402
from pitlake.information import enumerate_items, feature_rows, in_reach, sessions  # noqa: E402
from pitlake.jobs.decide import Spec, decide  # noqa: E402
from pitlake.jobs.information_set import Loaded, rebuilt_inputs  # noqa: E402

MIDDLE_BAR = "bar:BTCUSDT/2025-01-12"
TIMES = decision_times(date(2025, 1, 10), date(2025, 2, 25))


@pytest.fixture(scope="module")
def loaded(spark, world):
    from pyspark.sql import functions as F

    features = build_features(spark, world["bars_1d"], world["facts"], world["vintages"])
    bars = world["bars_1d"].select("*", *(F.col("close").alias(c) for c in ("open", "high", "low")))
    facts = fundamentals(spark, world["facts"])
    return Loaded(
        {"gold.features": 1},
        features,
        bars,
        facts,
        enumerate_items(bars, facts, features, HARNESS_REACH),
        TIMES,
        feature_rows(in_reach(features, HARNESS_REACH)),
    )


def test_the_pure_python_as_of_equals_spark(spark, loaded):
    spark_inputs = rebuilt_inputs(spark, loaded.rows, TIMES)
    assert sorted(map(repr, inputs_at(loaded.rows, TIMES))) == sorted(map(repr, spark_inputs))


def test_it_also_equals_spark_in_an_ablated_world(spark, loaded):
    removal = plan(loaded.rows, [MIDDLE_BAR], sessions(loaded.items))
    ablated = apply(loaded.rows, removal, recompute(loaded.bars, removal, HARNESS_REACH))
    assert inputs_hash(inputs_at(ablated, TIMES)) == inputs_hash(
        rebuilt_inputs(spark, ablated, TIMES)
    )


def test_the_strategy_decides_identically_and_checks_its_inputs(spark, loaded):
    recorded = inputs_hash(rebuilt_inputs(spark, loaded.rows, TIMES))
    record = {
        "backtest_id": "b",
        "research_run_id": "r",
        "period_start": TIMES[0].date(),
        "period_end": TIMES[-1].date(),
        "input_hash": recorded,
        "params": GRID[0].to_json(),
    }
    found = decide(Spec("strategy", record, None, (), 3, 2), loaded, None, 1)
    assert found.verified is True and found.summary.agreement == 1.0
    assert found.passed(1.0) and found.summary.distinct == 1
    withheld = decide(Spec("strategy", record, None, (MIDDLE_BAR,), 2, 2), loaded, None, 1)
    assert withheld.verified is None and withheld.passed(1.0)
    assert withheld.set_hash != found.set_hash

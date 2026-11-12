import json
import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.attribution import EVALUATED, SAMPLED  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH, STRATEGY_REACH, in_reach, inputs_at  # noqa: E402
from pitlake.harness import inputs_hash as strategy_inputs_hash  # noqa: E402
from pitlake.information import enumerate_items, feature_rows  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.attribute import make_point  # noqa: E402
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.jobs.suffice import Builder  # noqa: E402
from pitlake.jobs.supply import (  # noqa: E402
    MEASURED,
    PARTIAL,
    parse_args,
    stored_tried,
    supply_point,
)
from pitlake.monotonicity import outcome  # noqa: E402
from pitlake.tables import (  # noqa: E402
    ABLATIONS,
    ABLATIONS_SCHEMA,
    DECISION_SAMPLES,
    MONOTONICITY_CURVES,
    MONOTONICITY_CURVES_SCHEMA,
    MONOTONICITY_TESTS,
    MONOTONICITY_TESTS_SCHEMA,
)

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
DAY = date(2025, 1, 15)
AT = datetime(2025, 3, 1, tzinfo=UTC)
SHARES = ["--levels", "0 0.5 1", "--draws", "2", "--days", "7"]


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


class Collect:
    def __init__(self):
        self.rows: dict[str, list[tuple]] = {}

    def add(self, table: str, rows) -> None:
        self.rows.setdefault(table, []).extend(rows)

    def flush(self, force: bool = True) -> None:
        pass

    def named(self, table: str, schema: str) -> list[dict]:
        return [dict(zip(columns(schema), r, strict=True)) for r in self.rows.get(table, [])]


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
        feature_rows(spark_in_reach(features, HARNESS_REACH)),
    )


@pytest.fixture(scope="module")
def record(loaded):
    rows = in_reach(loaded.rows, STRATEGY_REACH)
    return {
        "backtest_id": "b",
        "research_run_id": "r",
        "period_start": TIMES[0].date(),
        "period_end": TIMES[-1].date(),
        "input_hash": strategy_inputs_hash(inputs_at(rows, TIMES)),
        "params": GRID[0].to_json(),
    }


def run(spark, loaded, point, builder, argv, complete=None, tried=None, deadline=None):
    writer = Collect()
    args = parse_args(["--catalog", "c", "--run-id", "run", *SHARES, *argv])
    found = supply_point(
        spark,
        point,
        loaded,
        builder,
        tried or {},
        None,
        loaded.rows,
        complete,
        args,
        writer,
        time.monotonic() + 600 if deadline is None else deadline,
        AT,
    )
    return found, writer


@pytest.fixture(scope="module")
def strategy(spark, loaded, record):
    argv = ["--deciders", "strategy"]
    args = parse_args(["--catalog", "c", *SHARES, *argv])
    point = make_point("strategy", record, DAY, loaded.versions, args)
    builder = Builder(loaded)
    found, writer = run(spark, loaded, point, builder, argv)
    return point, builder, found, writer, argv


def test_the_strategy_is_scored_in_every_world_of_every_draw(loaded, strategy):
    point, _, found, writer, _ = strategy
    assert found.state == MEASURED, found.problems
    tests = writer.named(MONOTONICITY_TESTS, MONOTONICITY_TESTS_SCHEMA)
    assert len(tests) == 4 and len({t["kept_hash"] for t in tests}) == 4
    assert all(t["rebuilt"] and t["status"] == EVALUATED for t in tests)
    assert sorted(t["kept_count"] for t in tests)[0] == 0
    rows = writer.named(MONOTONICITY_CURVES, MONOTONICITY_CURVES_SCHEMA)
    assert [(r["draw"], r["fraction"]) for r in rows] == [
        (0, 0.0), (0, 0.5), (0, 1.0), (1, 0.0), (1, 0.5), (1, 1.0),
    ]  # fmt: skip
    assert all(r["quality"] == 0.0 for r in rows if r["fraction"] == 0.0)
    whole = [r["quality"] for r in rows if r["fraction"] == 1.0]
    assert whole[0] == whole[1] and rows[0]["outcome_days"] == 7
    assert rows[0]["returns"] == pytest.approx(outcome(loaded.rows, point.at, 7).returns)
    (ablation,) = writer.named(ABLATIONS, ABLATIONS_SCHEMA)
    assert ablation["ablation_id"] == rows[0]["ablation_id"] and ablation["coherent"]
    assert DECISION_SAMPLES not in writer.rows


def test_a_later_run_reuses_every_world_and_a_late_one_stops(spark, loaded, strategy):
    point, builder, found, writer, argv = strategy
    tests = writer.named(MONOTONICITY_TESTS, MONOTONICITY_TESTS_SCHEMA)
    tried = {t["kept_hash"]: stored_tried(t) for t in tests}
    again, rewritten = run(spark, loaded, point, builder, argv, tried=tried)
    assert again.state == MEASURED and MONOTONICITY_TESTS not in rewritten.rows
    assert again.earlier == 4 and again.chains() == found.chains()
    some = dict(list(tried.items())[:2])
    late, nothing = run(spark, loaded, point, builder, argv, tried=some,
                        deadline=time.monotonic() - 1)  # fmt: skip
    assert late.state == PARTIAL and late.links == () and nothing.rows == {}


def scripted(messages: list[dict], tools: list[dict] | None) -> dict:
    results = [m["content"] for m in messages if m["role"] == "tool"]
    if not results:
        call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
        return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
    falling = "Change over 1 session -" in results[-1]
    ratings = {s: "neutral" for s in ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")}
    ratings["AAPL"] = "underweight" if falling else "overweight"
    return {"content": json.dumps(ratings)}


def test_the_agent_is_scored_by_the_tilt_of_every_sample(spark, loaded, record):
    argv = ["--endpoint", "e", "--samples", "2", "--verify", "none"]
    args = parse_args(["--catalog", "c", *SHARES, *argv])
    point = make_point("agent", record, DAY, loaded.versions, args)
    found, writer = run(spark, loaded, point, Builder(loaded), argv, complete=scripted)
    assert found.state == MEASURED, found.problems
    tests = writer.named(MONOTONICITY_TESTS, MONOTONICITY_TESTS_SCHEMA)
    assert all(t["status"] == SAMPLED and t["rebuilt"] is None for t in tests)
    assert len(writer.rows[DECISION_SAMPLES]) == 2 * len(tests)
    returns = found.outcome.returns
    mean = sum(returns.values()) / len(returns)
    full = max(tests, key=lambda t: t["kept_count"])
    (decision,) = set(full["decisions"])
    stance = {"overweight": 1, "underweight": -1}[json.loads(decision)["AAPL"]]
    assert full["quality"] == pytest.approx(stance * (returns["AAPL"] - mean) / 5)
    empty = min(tests, key=lambda t: t["kept_count"])
    assert json.loads(empty["decision"])["AAPL"] == "overweight"
    assert empty["quality"] > 0 > full["quality"]
    assert all(a["coherent"] for a in writer.named(ABLATIONS, ABLATIONS_SCHEMA))

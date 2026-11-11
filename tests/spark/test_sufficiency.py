import json
import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.attribution import SAMPLED, SCREENED, strategy_effect  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import (  # noqa: E402
    HARNESS_REACH,
    STRATEGY_REACH,
    in_reach,
    inputs_at,
    strategy_sample,
)
from pitlake.harness import inputs_hash as strategy_inputs_hash  # noqa: E402
from pitlake.information import enumerate_items, feature_rows  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.attribute import make_point, stored_base  # noqa: E402
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.jobs.suffice import (  # noqa: E402
    PARTIAL,
    SUFFICIENT,
    Builder,
    parse_args,
    stored_tried,
    suffice_point,
)
from pitlake.sufficiency import MINIMAL  # noqa: E402
from pitlake.tables import (  # noqa: E402
    ABLATION_CHANGES,
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    SUFFICIENCY_TESTS,
    SUFFICIENCY_TESTS_SCHEMA,
    SUFFICIENT_SETS,
    SUFFICIENT_SETS_SCHEMA,
)

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
AT = datetime(2025, 3, 1, tzinfo=UTC)
WINDOW = [f"2025-01-{d:02d}" for d in range(6, 27)]


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


def run(spark, loaded, point, builder, argv, complete=None, base=None, tried=None, deadline=None):
    writer = Collect()
    args = parse_args(["--catalog", "c", "--run-id", "run", *argv])
    found = suffice_point(
        spark,
        point,
        loaded,
        builder,
        base,
        tried or {},
        None,
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
    point = make_point(
        "strategy", record, None, loaded.versions, parse_args(["--catalog", "c", *argv])
    )
    builder = Builder(loaded)
    found, writer = run(spark, loaded, point, builder, argv)
    return point, builder, found, writer, argv


def test_the_strategy_needs_the_last_window_of_each_symbol_it_holds(spark, loaded, strategy):
    point, builder, found, writer, _ = strategy
    assert found.state == SUFFICIENT, found.problems
    settled = found.settled
    expected = {f"bar:{s}/{d}" for s in ("AAPL", "BTCUSDT") for d in WINDOW}
    assert set(settled.kept) == expected
    assert settled.confirmed and settled.rebuilt is True and settled.coherent is True
    assert found.outline == (
        "AAPL: 21 bars 2025-01-06 to 2025-01-26",
        "BTCUSDT: 21 bars 2025-01-06 to 2025-01-26",
    )
    (base_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    base = stored_base(base_row)
    assert base.settled["AAPL"] > 0 and base.settled["BTCUSDT"] > 0
    assert base.settled["MSFT"] == 0
    for gone in ("bar:AAPL/2025-01-06", "bar:BTCUSDT/2025-01-26"):
        world = builder.build(spark, set(expected) - {gone})
        sample = strategy_sample(builder.rows(world, STRATEGY_REACH), point.times, point.params)
        assert strategy_effect(base, sample.decision).changed
    (set_row,) = writer.named(SUFFICIENT_SETS, SUFFICIENT_SETS_SCHEMA)
    assert set_row["kept_by_kind"] == {"bar": 42, "filing": 0, "release": 0}
    assert set_row["candidates"] == found.candidates and set_row["sampled"] == 0
    (ablation,) = writer.named(ABLATIONS, ABLATIONS_SCHEMA)
    assert ablation["ablation_id"] == settled.ablation_id and ablation["coherent"]
    assert set(ablation["withheld"]).isdisjoint(expected) and writer.rows[ABLATION_CHANGES]
    tests = writer.named(SUFFICIENCY_TESTS, SUFFICIENCY_TESTS_SCHEMA)
    assert len(tests) == settled.tests and tests[0]["kept_count"] == 0
    assert not tests[0]["passed"] and any(t["phase"] == MINIMAL for t in tests)
    assert DECISION_SAMPLES not in writer.rows


def test_a_later_run_continues_from_the_worlds_already_tried(spark, loaded, strategy):
    point, builder, found, writer, argv = strategy
    tests = writer.named(SUFFICIENCY_TESTS, SUFFICIENCY_TESTS_SCHEMA)
    (base_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    tried = {t["kept_hash"]: stored_tried(t) for t in tests}
    again, rewritten = run(
        spark, loaded, point, builder, argv, base=stored_base(base_row), tried=tried
    )
    assert again.state == SUFFICIENT and again.settled.kept == found.settled.kept
    assert SUFFICIENCY_TESTS not in rewritten.rows and again.earlier == len(tests)
    assert len(rewritten.rows[SUFFICIENT_SETS]) == 1
    late, nothing = run(spark, loaded, point, builder, argv, deadline=time.monotonic() - 1)
    assert late.state == PARTIAL and late.base is None and nothing.rows == {}
    stopped, partial = run(
        spark, loaded, point, builder, argv, base=stored_base(base_row),
        tried=dict(list(tried.items())[:10]), deadline=time.monotonic() - 1,
    )  # fmt: skip
    assert stopped.state == PARTIAL and stopped.settled is None and partial.rows == {}


def scripted(messages: list[dict], tools: list[dict] | None) -> dict:
    results = [m["content"] for m in messages if m["role"] == "tool"]
    if not results:
        call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
        return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
    falling = "Change over 1 session -" in results[-1]
    ratings = {s: "neutral" for s in ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")}
    ratings["AAPL"] = "underweight" if falling else "overweight"
    return {"content": json.dumps(ratings)}


def test_the_agent_needs_only_the_two_bars_its_rating_reads(spark, loaded, record):
    argv = ["--endpoint", "e", "--samples", "3"]
    point = make_point(
        "agent", record, None, loaded.versions, parse_args(["--catalog", "c", *argv])
    )
    found, writer = run(spark, loaded, point, Builder(loaded), argv, complete=scripted)
    assert found.state == SUFFICIENT, found.problems
    assert found.base.settled["AAPL"] == "underweight"
    assert set(found.settled.kept) == {"bar:AAPL/2025-01-25", "bar:AAPL/2025-01-26"}
    assert found.settled.confirmed and found.settled.coherent and found.settled.rebuilt
    tests = writer.named(SUFFICIENCY_TESTS, SUFFICIENCY_TESTS_SCHEMA)
    statuses = {t["status"] for t in tests}
    assert statuses == {SAMPLED, SCREENED}
    assert all(t["passed"] for t in tests if t["status"] == SCREENED)
    assert any(t["stopped"] and t["samples"] < 3 for t in tests)
    screened_btc = [t for t in tests if t["status"] == SCREENED and t["dropped"]
                    and all(d.startswith("bar:BTCUSDT") for d in t["dropped"])]  # fmt: skip
    assert screened_btc
    (set_row,) = writer.named(SUFFICIENT_SETS, SUFFICIENT_SETS_SCHEMA)
    assert set_row["sampled"] == sum(t["status"] == SAMPLED for t in tests if t["valid"])
    confirm = [r for r in writer.rows[DECISION_SAMPLES] if r[0] == set_row["confirm_id"]]
    assert len(confirm) == 2 * 3

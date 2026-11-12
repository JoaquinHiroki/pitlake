import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.attribution import SAMPLED  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH, STRATEGY_REACH, in_reach, inputs_at  # noqa: E402
from pitlake.harness import inputs_hash as strategy_inputs_hash  # noqa: E402
from pitlake.horizon import FULL, REFINE  # noqa: E402
from pitlake.information import enumerate_items, feature_rows  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.attribute import make_point, stored_base  # noqa: E402
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.jobs.suffice import Builder  # noqa: E402
from pitlake.jobs.truncate import (  # noqa: E402
    BREAKS,
    HOLDS,
    MEASURED,
    PARTIAL,
    parse_args,
    stored_curves,
    stored_tried,
    truncate_point,
)
from pitlake.tables import (  # noqa: E402
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    DECISION_SAMPLES,
    HORIZON_CURVES,
    HORIZON_CURVES_SCHEMA,
    HORIZON_TESTS,
    HORIZON_TESTS_SCHEMA,
)

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
AT = datetime(2025, 3, 1, tzinfo=UTC)
LADDER = ["--horizons", "60 30 21 14 7 3 1 0"]


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


def run(spark, loaded, point, builder, argv, complete=None, base=None, tried=None, stored=None,
        deadline=None):  # fmt: skip
    writer = Collect()
    args = parse_args(["--catalog", "c", "--run-id", "run", *LADDER, *argv])
    found = truncate_point(
        spark,
        point,
        loaded,
        builder,
        base,
        tried or {},
        stored,
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


def test_the_strategy_holds_while_its_last_window_is_kept(strategy):
    point, _, found, writer, _ = strategy
    assert found.state == MEASURED, found.problems
    curve = found.curve
    assert (curve.holds_to, curve.breaks_at) == (21, 20) and curve.recovers == ()
    (base_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    base = stored_base(base_row)
    held = curve.parts()
    assert held["AAPL"] == 21 and held["BTCUSDT"] == 21
    assert all(held[s] == 0 for s, w in base.settled.items() if w == 0)
    rows = writer.named(HORIZON_CURVES, HORIZON_CURVES_SCHEMA)
    assert [r["horizon_days"] for r in rows][:3] == [None, 60, 30]
    assert rows[0]["phase"] == FULL and rows[0]["reproduced"]
    assert {r["phase"] for r in rows if r["horizon_days"] in (20, 17)} <= {REFINE}
    assert all(r["holds_to"] == 21 and r["breaks_at"] == 20 for r in rows)
    bounds = {r["boundary"]: r for r in rows if r["boundary"]}
    assert bounds[HOLDS]["horizon_days"] == 21 and bounds[BREAKS]["horizon_days"] == 20
    ablations = writer.named(ABLATIONS, ABLATIONS_SCHEMA)
    assert [a["ablation_id"] for a in ablations] == [
        bounds[HOLDS]["ablation_id"],
        bounds[BREAKS]["ablation_id"],
    ]
    assert all(a["coherent"] for a in ablations)
    tests = writer.named(HORIZON_TESTS, HORIZON_TESTS_SCHEMA)
    assert all(t["rebuilt"] for t in tests) and len(tests) == len({t["kept_hash"] for t in tests})
    assert tests[0]["horizon_days"] is None and tests[-1]["kept_count"] < tests[0]["kept_count"]
    assert DECISION_SAMPLES not in writer.rows


def test_a_later_run_reads_the_curve_or_continues_it(spark, loaded, strategy):
    point, builder, found, writer, argv = strategy
    tests = writer.named(HORIZON_TESTS, HORIZON_TESTS_SCHEMA)
    (base_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    base = stored_base(base_row)
    tried = {t["kept_hash"]: stored_tried(t) for t in tests}
    again, rewritten = run(spark, loaded, point, builder, argv, base=base, tried=tried)
    assert again.state == MEASURED and again.curve.holds_to == 21
    assert HORIZON_TESTS not in rewritten.rows and again.earlier == len(tests)
    stored = stored_curves(writer.named(HORIZON_CURVES, HORIZON_CURVES_SCHEMA))[point.key]
    read, nothing = run(spark, loaded, point, builder, argv, base=base, tried=tried,
                        stored=stored)  # fmt: skip
    assert read.state == MEASURED and nothing.rows == {}
    assert [s.days for s in read.steps] == [s.days for s in found.steps]
    some = dict(list(tried.items())[:3])
    late, nothing = run(
        spark, loaded, point, builder, argv, base=base, tried=some, deadline=time.monotonic() - 1
    )
    assert late.state == PARTIAL and late.curve is None and nothing.rows == {}


def scripted(messages: list[dict], tools: list[dict] | None) -> dict:
    import json

    results = [m["content"] for m in messages if m["role"] == "tool"]
    if not results:
        call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
        return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
    falling = "Change over 1 session -" in results[-1]
    ratings = {s: "neutral" for s in ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")}
    ratings["AAPL"] = "underweight" if falling else "overweight"
    return {"content": json.dumps(ratings)}


def test_the_agent_holds_while_the_two_bars_its_rating_reads_are_kept(spark, loaded, record):
    argv = ["--endpoint", "e", "--samples", "3", "--verify", "boundary"]
    point = make_point(
        "agent", record, None, loaded.versions, parse_args(["--catalog", "c", *argv])
    )
    found, writer = run(spark, loaded, point, Builder(loaded), argv, complete=scripted)
    assert found.state == MEASURED, found.problems
    assert found.base.settled["AAPL"] == "underweight"
    curve = found.curve
    assert (curve.holds_to, curve.breaks_at) == (2, 1)
    assert curve.parts()["AAPL"] == 2 and curve.parts()["MSFT"] == 0
    tests = writer.named(HORIZON_TESTS, HORIZON_TESTS_SCHEMA)
    assert SAMPLED in {t["status"] for t in tests}
    assert all(t["rebuilt"] is None for t in tests)
    broken = [t for t in tests if not t["reproduced"]]
    assert broken and all(t["outcomes"]["AAPL"] == "flipped" for t in broken)
    assert all(t["agreement"] == pytest.approx(0.8) for t in broken)
    sampled = [t for t in tests if t["status"] == SAMPLED]
    assert len(writer.rows[DECISION_SAMPLES]) == 2 * 3 + 3 * len(sampled)
    assert all(a["coherent"] for a in writer.named(ABLATIONS, ABLATIONS_SCHEMA))


def test_a_world_where_the_endpoint_failed_is_decided_again(spark, loaded, record):
    argv = ["--endpoint", "e", "--samples", "2", "--verify", "boundary", "--horizons", "3 0"]

    def failing(messages, tools):
        results = [m["content"] for m in messages if m["role"] == "tool"]
        if results and results[-1].startswith("No close"):
            raise RuntimeError("endpoint down")
        return scripted(messages, tools)

    point = make_point(
        "agent", record, None, loaded.versions, parse_args(["--catalog", "c", *argv])
    )
    builder = Builder(loaded)
    found, writer = run(spark, loaded, point, builder, argv, complete=failing)
    assert found.state == PARTIAL and found.errors == 1 and HORIZON_CURVES not in writer.rows
    tests = writer.named(HORIZON_TESTS, HORIZON_TESTS_SCHEMA)
    assert [t["errors"] for t in tests if not t["valid"]] == [2]
    (base_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    tried = {t["kept_hash"]: stored_tried(t) for t in tests if t["valid"]}
    again, rewritten = run(spark, loaded, point, builder, argv, complete=scripted,
                           base=stored_base(base_row), tried=tried)  # fmt: skip
    assert again.state == MEASURED and again.curve.breaks_at is not None
    assert len(rewritten.named(HORIZON_TESTS, HORIZON_TESTS_SCHEMA)) == 1

import json
import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.ablation import apply, plan, recompute, recompute_each  # noqa: E402
from pitlake.attribution import EVALUATED, FLIPPED, SAME, SAMPLED, SCREENED  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH, STRATEGY_REACH, in_reach, inputs_at  # noqa: E402
from pitlake.harness import inputs_hash as strategy_inputs_hash  # noqa: E402
from pitlake.information import enumerate_items, feature_rows, sessions  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.attribute import (  # noqa: E402
    ATTRIBUTED,
    PARTIAL,
    Worlds,
    attribute_point,
    make_point,
    parse_args,
    stored_base,
    stored_record,
)
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.tables import (  # noqa: E402
    ATTRIBUTION_POINTS,
    ATTRIBUTION_POINTS_SCHEMA,
    ATTRIBUTIONS,
    ATTRIBUTIONS_SCHEMA,
    DECISION_SAMPLES,
)

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
AT = datetime(2025, 3, 1, tzinfo=UTC)
LAST_AAPL = "bar:AAPL/2025-01-26"
BARS = ["bar:BTCUSDT/2025-01-05", "bar:BTCUSDT/2025-01-12", "bar:AAPL/2025-01-20", LAST_AAPL]


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


def test_one_batch_recomputes_every_bar_as_one_at_a_time_would(spark, loaded):
    days = sessions(loaded.items)
    plans = {b: plan(loaded.rows, [b], days) for b in BARS}
    batched = recompute_each(spark, loaded.bars, {b: p for b, p in plans.items() if p.inputs})
    assert set(batched) == set(BARS[:-1]) and all(batched.values())
    for bar in BARS[:-1]:
        alone = recompute(loaded.bars, plans[bar], HARNESS_REACH)
        assert sorted(map(repr, batched[bar])) == sorted(map(repr, alone))
    worlds = Worlds(loaded)
    worlds.prepare(spark, BARS)
    for bar in BARS:
        one = apply(loaded.rows, plans[bar], batched.get(bar, []))
        assert sorted(map(repr, worlds.rows(bar, HARNESS_REACH))) == sorted(map(repr, one))
        assert worlds.rebuilt(bar)
    assert worlds.rebuilt(None)


def run(spark, loaded, point, worlds, complete=None, base=None, records=None, deadline=None):
    writer = Collect()
    args = parse_args(
        ["--catalog", "c", "--endpoint", "e", "--samples", "3", "--bars", "3", "--run-id", "run"]
    )
    found = attribute_point(
        spark,
        point,
        loaded,
        worlds,
        base,
        records or {},
        complete,
        args,
        writer,
        time.monotonic() + 600 if deadline is None else deadline,
        AT,
    )
    return found, writer


def test_the_strategy_attributes_every_item_and_its_null_changes_nothing(spark, loaded, record):
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    point = make_point("strategy", record, None, loaded.versions, args)
    found, writer = run(spark, loaded, point, Worlds(loaded))
    assert found.state == ATTRIBUTED, found.problems
    assert found.left == 0 and found.candidates == len(found.records) - 1
    (point_row,) = writer.named(ATTRIBUTION_POINTS, ATTRIBUTION_POINTS_SCHEMA)
    assert point_row["inputs_verified"] is True and point_row["reproducible"]
    assert set(json.loads(point_row["settled"])) == {"AAPL", "BTCUSDT", "MSFT", "NVDA", "SPY"}
    null = found.null
    assert set(null.effect.outcomes.values()) == {SAME} and null.rebuilt is True
    assert not null.inputs_changed
    last = found.records[(LAST_AAPL,)]
    assert last.status == EVALUATED and last.inputs_changed and "AAPL" in last.effect.caused
    assert last.effect.outcomes["BTCUSDT"] == SAME and last.rebuilt is True
    caused = [r for r in found.records.values() if r.effect.caused]
    assert caused and all(r.rebuilt is True for r in caused)
    unchanged = [r for r in found.records.values() if r.withheld and not r.effect.changed]
    assert unchanged and all(r.rebuilt is None for r in unchanged)
    assert DECISION_SAMPLES not in writer.rows

    rows = writer.named(ATTRIBUTIONS, ATTRIBUTIONS_SCHEMA)
    again, rewritten = run(
        spark,
        loaded,
        point,
        Worlds(loaded),
        base=stored_base(point_row),
        records={tuple(r["withheld"]): stored_record(r) for r in rows},
    )
    assert again.state == ATTRIBUTED and rewritten.rows == {}


def test_a_late_run_stops_and_leaves_the_rest(spark, loaded, record):
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    point = make_point("strategy", record, None, loaded.versions, args)
    found, writer = run(spark, loaded, point, Worlds(loaded), deadline=time.monotonic() - 1)
    assert found.state == PARTIAL and found.base is None and writer.rows == {}


def scripted(messages: list[dict], tools: list[dict] | None) -> dict:
    results = [m["content"] for m in messages if m["role"] == "tool"]
    if not results:
        call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
        return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
    rising = "Change over 1 session +" in results[-1]
    ratings = {s: "neutral" for s in ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")}
    ratings["AAPL"] = "overweight" if rising else "underweight"
    return {"content": json.dumps(ratings)}


def test_the_agent_samples_only_worlds_that_change_what_its_tools_return(spark, loaded, record):
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--samples", "3"])
    point = make_point("agent", record, None, loaded.versions, args)
    found, writer = run(spark, loaded, point, Worlds(loaded), complete=scripted)
    assert found.state == ATTRIBUTED, found.problems
    assert found.base.settled["AAPL"] == "underweight" and found.base.calls == (
        ("symbol_snapshot", '{"symbol":"AAPL"}'),
    )
    null = found.null
    assert null.status == SAMPLED and set(null.effect.outcomes.values()) == {SAME}
    assert null.rebuilt is True
    last = found.records[(LAST_AAPL,)]
    assert last.status == SAMPLED and last.effect.outcomes["AAPL"] == FLIPPED
    assert last.effect.caused == ("AAPL",) and last.rebuilt is True
    screened = [r for r in found.records.values() if r.status == SCREENED]
    assert screened and all(not r.inputs_changed for r in screened)
    assert all(r.withheld[0].startswith("bar:BTCUSDT") is False for r in found.records.values()
               if r.status == SAMPLED and r.withheld)  # fmt: skip
    sampled = [r for r in found.records.values() if r.status == SAMPLED and r.withheld]
    assert len(sampled) == found.selected and found.selected < found.changed
    samples = writer.rows[DECISION_SAMPLES]
    assert len(samples) == 2 * 3 + 3 * (1 + len(sampled))

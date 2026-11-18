import json
import os
import re
import time
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.attribution import EVALUATED, SAMPLED, SCREENED  # noqa: E402
from pitlake.backtest import GRID, decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH, STRATEGY_REACH, in_reach, inputs_at  # noqa: E402
from pitlake.harness import inputs_hash as strategy_inputs_hash  # noqa: E402
from pitlake.information import enumerate_items, feature_rows, information_set  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.attribute import make_point  # noqa: E402
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.jobs.value import (  # noqa: E402
    VALUED,
    Worlds,
    parse_args,
    stored_judged,
    value_point,
    values_of,
    venues_of,
)
from pitlake.tables import (  # noqa: E402
    ABLATIONS,
    ABLATIONS_SCHEMA,
    ATTRIBUTION_POINTS,
    DECISION_SAMPLES,
    SOURCE_TESTS,
    SOURCE_TESTS_SCHEMA,
)
from pitlake.value import CHANGES, EVERY, NOTHING, sources  # noqa: E402

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
AT = datetime(2025, 3, 1, tzinfo=UTC)


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


def found_sources(loaded, args):
    known = information_set(loaded.items, loaded.times[-1]).items.values()
    return tuple(sources(known, venues_of(loaded.bars), args.levels))


def run(spark, loaded, point, args, complete=None, tested=None, deadline=None):
    writer = Collect()
    worlds = Worlds(spark, loaded, args, writer, AT)
    late = time.monotonic() + 600 if deadline is None else deadline
    found = found_sources(loaded, args)
    progress = value_point(point, loaded, worlds, found, None, tested or {}, [], complete, args,
                           writer, late, AT)  # fmt: skip
    return progress, writer


def test_withholding_each_feed_changes_only_what_the_strategy_reads(spark, loaded, record):
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    assert venues_of(loaded.bars) == {"AAPL": "alpaca", "BTCUSDT": "binance"}
    names = [s.name for s in found_sources(loaded, args)]
    assert names == ["alpaca", "binance", "fred", "fred/CPIAUCSL", "fred/UNRATE"]
    point = make_point("strategy", record, None, loaded.versions, args)
    found, writer = run(spark, loaded, point, args)
    assert found.state == VALUED, found.problems
    assert found.base.reproducible and found.null.status == EVALUATED
    assert ATTRIBUTION_POINTS in writer.rows and DECISION_SAMPLES not in writer.rows
    rows = writer.named(SOURCE_TESTS, SOURCE_TESTS_SCHEMA)
    by_source = {r["source"]: r for r in rows}
    assert by_source["alpaca"]["caused"] == ["AAPL"] and by_source["alpaca"]["inputs_changed"]
    assert by_source["alpaca"]["rows_removed"] > 0 and by_source["alpaca"]["knowable"] > 20
    assert by_source["binance"]["caused"] == ["BTCUSDT"]
    for quiet in ("fred", "fred/CPIAUCSL", "fred/UNRATE"):
        assert by_source[quiet]["caused"] == [] and set(by_source[quiet]["outcomes"].values()) == {
            "same"
        }
    assert all(r["rebuilt"] and r["coherent"] for r in rows)
    ablations = writer.named(ABLATIONS, ABLATIONS_SCHEMA)
    assert len(ablations) == len(rows) and all(a["coherent"] for a in ablations)
    values = {(v.decider, v.source.name): v for v in values_of([found], found.worlds[1:])}
    assert values[("strategy", "alpaca")].verdict == CHANGES
    assert values[("strategy", "binance")].verdict == CHANGES
    assert values[("strategy", "fred")].verdict == NOTHING
    assert (EVERY, "fred") not in values

    stored = {r["withheld_hash"]: stored_judged(r) for r in rows}
    again, rewritten = run(spark, loaded, point, args, tested=stored)
    assert again.state == VALUED and set(rewritten.rows) == {ATTRIBUTION_POINTS}


def scripted(messages: list[dict], tools: list[dict] | None) -> dict:
    results = [m["content"] for m in messages if m["role"] == "tool"]
    if not results:
        call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
        return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
    rising = "Change over 1 session +" in results[-1]
    ratings = {s: "neutral" for s in ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")}
    ratings["AAPL"] = "overweight" if rising else "underweight"
    return {"content": json.dumps(ratings)}


def test_the_agent_is_sampled_only_without_the_sources_its_tools_read(spark, loaded, record):
    args = parse_args(
        ["--catalog", "c", "--endpoint", "e", "--deciders", "agent", "--samples", "3",
         "--levels", "feed", "--run-id", "run"]
    )  # fmt: skip
    point = make_point("agent", record, None, loaded.versions, args)
    found, writer = run(spark, loaded, point, args, complete=scripted)
    assert found.state == VALUED, found.problems
    statuses = {j.source: j.status for j in found.tested.values()}
    assert statuses == {
        "none": SAMPLED, "alpaca": SAMPLED, "binance": SCREENED, "fred": SCREENED,
    }  # fmt: skip
    assert all(j.check.coherent and j.check.rebuilt for j in found.tested.values())
    samples = writer.rows[DECISION_SAMPLES]
    assert len(samples) == 2 * 3 + 2 * 3

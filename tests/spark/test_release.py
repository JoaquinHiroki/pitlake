import json
import os
from datetime import date
from types import SimpleNamespace

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from pitlake.backtest import decision_times  # noqa: E402
from pitlake.gold import build_features, fundamentals  # noqa: E402
from pitlake.harness import HARNESS_REACH  # noqa: E402
from pitlake.information import enumerate_items, feature_rows, information_set  # noqa: E402
from pitlake.information import in_reach as spark_in_reach  # noqa: E402
from pitlake.jobs.information_set import Loaded  # noqa: E402
from pitlake.jobs.release import describe, release_one, release_without  # noqa: E402
from pitlake.jobs.suffice import Builder  # noqa: E402
from pitlake.release import RESTRICTED, SHARE_ALIKE  # noqa: E402
from pitlake_counterfactual import cli  # noqa: E402
from pitlake_counterfactual.world import Bundle  # noqa: E402

TIMES = decision_times(date(2025, 1, 2), date(2025, 1, 27))
RECORD = {
    "backtest_id": "a4d4c4e0955046b8a6fb6a171f8b1071",
    "research_run_id": "run",
    "period_start": date(2025, 1, 2),
    "period_end": date(2025, 1, 27),
}
ARGS = SimpleNamespace(catalog="pitlake_test", code_commit="abc")


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
def released(loaded, tmp_path_factory):
    return release_one(loaded, RECORD, None, str(tmp_path_factory.mktemp("releases")), 2, ARGS)


@pytest.fixture(scope="module")
def public(spark, loaded, released, tmp_path_factory):
    args = SimpleNamespace(**vars(ARGS), without=("alpaca",))
    out = str(tmp_path_factory.mktemp("public"))
    return release_without(spark, Builder(loaded), released, RECORD, None, out, 2, args)


def test_the_bundle_without_alpaca_is_the_full_one_without_its_bars(public, released, loaded):
    assert public.problems == (), public.problems
    assert describe(public).startswith("RELEASED: world-2025-01-27-a4d4c4e0-without-alpaca")
    assert f"is {released.name} without alpaca" in describe(public)
    assert public.licence == SHARE_ALIKE and released.licence == RESTRICTED
    bundle = Bundle.read(public.path)
    assert {b.venue for b in bundle.bars} == {"binance"}
    assert {r[0] for r in bundle.rows if r[5] == bundle.lineage.prices} == {"BTCUSDT"}
    assert bundle.source["without"] == ["alpaca"]
    assert bundle.source["full_set_hash"] == released.set_hash != public.set_hash
    assert public.withheld == sum(released.items.values()) - sum(public.items.values()) > 0
    assert {i.split(":")[0] for i in public.checked} >= {"bar", "release"}
    assert "alpaca" not in bundle.terms["sources"]


def test_the_engine_rebuilds_the_world_and_withholds_as_pitlake_does(released, loaded):
    assert released.problems == ()
    assert released.verified and describe(released).startswith("RELEASED: world-2025-01-27-a4d4")
    kinds = {i.split(":")[0] for i in released.checked}
    assert {"bar", "release"} <= kinds
    info = information_set(loaded.items, TIMES[-1])
    assert released.set_hash == info.hash
    assert sum(released.items.values()) == len(info.items)


def test_the_bundle_names_its_origin(released):
    bundle = Bundle.read(released.path)
    assert bundle.id == released.bundle_id and bundle.name == released.name
    assert bundle.source["backtest_id"] == RECORD["backtest_id"]
    assert bundle.source["table_versions"] == {"gold.features": 1}
    assert bundle.at == TIMES[-1] and bundle.check() == []


def test_a_bar_in_the_middle_of_a_window_is_recomputed_alike(released, loaded):
    from pitlake.jobs.decide import world
    from pitlake.release import owned_rows
    from pitlake_counterfactual.world import differences

    bundle = Bundle.read(released.path)
    days = bundle.days["AAPL"]
    item = f"bar:AAPL/{days[len(days) // 2]}"
    mine = bundle.withhold([item])
    assert mine.coherent and mine.recomputed > 0
    assert differences(mine.rows, owned_rows(world(loaded, (item,)), TIMES[-1])) == []


def test_the_released_command_ablates_the_exported_bundle(released, tmp_path, capsys):
    bundle = Bundle.read(released.path)
    last = f"bar:AAPL/{bundle.days['AAPL'][-1]}"
    argv = [
        "ablate",
        released.path,
        "--withhold",
        last,
        "--decider",
        "scripted:decide",
        "--samples",
        "2",
        "--out",
        str(tmp_path / "result"),
    ]
    with pytest.raises(SystemExit) as stopped:
        cli.main(argv)
    printed = capsys.readouterr().out
    assert stopped.value.code == 0, printed
    result = json.loads((tmp_path / "result" / "result.json").read_text())
    assert result["bundle"]["id"] == released.bundle_id and result["passed"]
    assert result["worlds"][0]["coherent"]


def test_a_bundle_that_is_not_the_full_one_without_the_feed_is_caught(public, released):
    from pitlake.jobs.release import check_against_full

    bundle = Bundle.read(public.path)
    assert check_against_full(bundle, released.path, frozenset()) != []
    full = Bundle.read(released.path)
    alpaca = frozenset(i for i in full.items if not i.startswith("bar:BTCUSDT/"))
    alpaca = frozenset(i for i in alpaca if i.startswith("bar:"))
    assert check_against_full(bundle, released.path, alpaca) == []

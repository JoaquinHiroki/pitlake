import gzip
import json
import re
from datetime import timedelta
from types import SimpleNamespace

import pytest

import fixture
import scripted
from pitlake import attribution, harness
from pitlake.ablation import plan
from pitlake.backtest import DECISION_HOUR
from pitlake.backtest import instant as pit_instant
from pitlake.harness import HARNESS_REACH, SYMBOLS, View, agent_tools, inputs_at, run_agent
from pitlake.information import (
    bar_items,
    filing_items,
    information_set,
    release_items,
    sessions,
)
from pitlake.jobs import release as job
from pitlake.release import FORMAT, LINEAGE, encode, name_of, payload, tool_calls, tool_texts
from pitlake.tables import WORLD_BUNDLES_SCHEMA
from pitlake_counterfactual import ablate as engine_ablate
from pitlake_counterfactual import agent as engine
from pitlake_counterfactual.world import Bundle, instant
from pitlake_counterfactual.world import inputs_hash as engine_inputs_hash

AT = fixture.AT


def when(us: int):
    return instant(us)


def bars():
    return [(*r[:-1], when(r[-1])) for r in fixture.bars()]


def filings():
    return [(*r[:-1], when(r[-1])) for r in fixture.filings()]


def releases():
    return [
        ("US", f, when(e), when(k), v, "fred_series_vintages", ref)
        for f, e, k, v, ref in fixture.releases()
    ]


def rows():
    found = fixture.rows(fixture.bars(), fixture.filings(), fixture.releases())
    return [(r[0], r[1], when(r[2]), when(r[3]), r[4], r[5], r[6]) for r in found]


def items():
    return (
        bar_items(bars(), HARNESS_REACH)
        + filing_items(filings(), HARNESS_REACH)
        + release_items(releases(), HARNESS_REACH)
    )


def built(extra_bars=(), extra_rows=()):
    info = information_set([*items(), *bar_items(extra_bars, HARNESS_REACH)], AT)
    source = {"catalog": "test", "backtest_id": "a4d4c4e0955046b8a6fb6a171f8b1071"}
    found = payload(
        AT,
        info.items,
        [*bars(), *extra_bars],
        filings(),
        releases(),
        [*rows(), *extra_rows],
        info.hash,
        source,
    )
    return info, found


def test_the_engine_uses_pitlake_lineage():
    assert LINEAGE["window_days"] == fixture.LINEAGE["window_days"]
    assert LINEAGE == fixture.LINEAGE
    assert FORMAT == "pitlake-world/1"


def test_the_engine_hashes_the_items_as_pitlake_does():
    info, found = built()
    bundle = Bundle(json.loads(json.dumps(found)))
    assert bundle.set_hash == info.hash
    assert {i.id: i.digest for i in bundle.items.values()} == {
        i.id: i.digest for i in info.items.values()
    }
    assert bundle.check() == []
    assert bundle.id == found["id"]


def test_a_bundle_holds_only_what_was_knowable():
    late_day = "2025-02-03"
    late = ("alpaca", "AAPL", late_day, "1", "1", "1", "1", "1", AT + timedelta(days=2))
    late_row = ("AAPL", "close", AT, AT + timedelta(days=2), 1.0, "bars_1d", late_day)
    info, found = built([late], [late_row])
    assert f"bar:AAPL/{late_day}" not in info.items
    assert not [b for b in found["bars"] if b[2] == late_day]
    assert not [r for r in found["rows"] if r[6] == late_day]
    assert found["name"] == "world-2025-02-01-a4d4c4e0" == name_of(AT, "a4d4c4e0aaaa")
    assert Bundle(found).check() == []


def test_a_bundle_encodes_the_same_way_twice():
    _, found = built()
    data = encode(found)
    assert data == encode(found) and json.loads(gzip.decompress(data)) == json.loads(
        json.dumps(found)
    )


def test_the_tools_read_the_same_text_in_both_engines():
    bundle = Bundle(built()[1])
    withheld = (f"filing:{fixture.FILING}", "release:UNRATE/2025-01-10", fixture.LAST_AAPL)
    for gone in ((), withheld):
        mine = engine.Session.of(bundle.withhold(gone))
        removed = set(plan(rows(), gone, sessions(items())).removed)
        texts = tool_texts([r for r in rows() if r not in removed], AT)
        for (name, arguments), key in zip(tool_calls(), texts, strict=True):
            assert mine.answer(name, arguments) == texts[key], key


def test_the_engine_removes_and_recomputes_what_pitlake_plans():
    bundle = Bundle(built()[1])
    for withheld in (("bar:AAPL/2025-01-02",), ("bar:BTCUSDT/2025-01-10", fixture.LAST_AAPL)):
        removal = plan(rows(), withheld, sessions(items()))
        world = bundle.withhold(withheld)
        assert world.removed == len(removal.removed) and world.coherent
        pairs = {(s, f, d) for s, found in removal.recompute.items() for f, d in found}
        before = set(bundle.rows)
        assert {(r[0], r[1], r[6]) for r in world.rows if r not in before} <= pairs
        assert 0 < world.recomputed <= len(pairs)


def test_the_task_tools_and_config_are_pitlake_s():
    assert engine.tools_for(SYMBOLS) == harness.TOOLS
    assert engine.decide_tool(SYMBOLS) == harness.DECIDE
    assert engine.prompts(AT, SYMBOLS) == harness.prompts(AT)
    for endpoint in ("databricks-meta-llama-3-3-70b-instruct", "databricks-llama-4-maverick"):
        mine = engine.agent_config(endpoint, SYMBOLS, endpoint in harness.ANSWERS_BY_TOOL)
        assert mine == harness.agent_config(endpoint)
    assert (engine.MAX_STEPS, engine.MAX_TOKENS, engine.TEMPERATURE) == (
        harness.MAX_STEPS,
        harness.MAX_TOKENS,
        harness.TEMPERATURE,
    )
    assert engine.AGENT_FEATURES == harness.AGENT_FEATURES
    assert AT.hour == DECISION_HOUR


@pytest.mark.parametrize(
    "text",
    [
        json.dumps({s: "Overweight" for s in SYMBOLS}),
        "no json here",
        json.dumps({**{s: "neutral" for s in SYMBOLS}, "TSLA": "neutral"}),
        json.dumps({s: "buy" for s in SYMBOLS}),
        "[1, 2]",
        "{not json}",
    ],
)
def test_answers_parse_alike(text):
    def outcome(parse):
        try:
            return parse()
        except ValueError as error:
            return str(error)

    assert outcome(lambda: engine.parse_decision(text, SYMBOLS)) == outcome(
        lambda: harness.parse_decision(text)
    )


def test_an_agent_decides_and_reads_alike_in_both_engines():
    bundle = Bundle(built()[1])
    mine = engine.run(engine.loop(scripted.complete), bundle.withhold(()))
    view = View(harness.in_reach(rows(), harness.AGENT_REACH), AT)
    theirs = run_agent(scripted.complete, agent_tools(view), AT)
    assert (mine.decision, mine.input_hash, mine.calls, mine.model_calls) == (
        theirs.decision,
        theirs.input_hash,
        theirs.calls,
        theirs.model_calls,
    )


def test_the_as_of_inputs_hash_alike():
    bundle = Bundle(built()[1])
    features = tuple(sorted({r[1] for r in rows()}))
    theirs = harness.inputs_hash(inputs_at(rows(), [AT], SYMBOLS, features))
    assert engine_inputs_hash(bundle.rows, SYMBOLS, "US", AT) == theirs


def test_settling_and_fisher_are_pitlake_s():
    for agree, total, other, others in ((10, 10, 0, 10), (7, 10, 3, 10), (20, 20, 9, 10)):
        assert engine_ablate.fisher(agree, total, other, others) == attribution.fisher(
            agree, total, other, others
        )
    decisions = [{"AAPL": "neutral"}] * 7 + [{"AAPL": "overweight"}] * 3
    assert engine_ablate.settle(decisions, ["AAPL"], 0.7) == harness.settle(
        decisions, ["AAPL"], 0.7
    )
    assert engine_ablate.UNSETTLED == harness.UNSETTLED and engine_ablate.ALPHA == attribution.ALPHA


def test_the_job_parses_its_parameters():
    args = job.parse_args(["--catalog", "pitlake_dev", "--at", "2026-06-01 2026-10-08"])
    assert args.out == "/Volumes/pitlake_dev/control/releases"
    assert [str(d) for d in args.at] == ["2026-06-01", "2026-10-08"] and args.check == 3
    assert job.parse_args(["--catalog", "pitlake_dev"]).at == (None,)
    with pytest.raises(SystemExit):
        job.parse_args(["--catalog", "pitlake_dev", "--check", "-1"])


def released(problems=()):
    return job.Released(
        "world-2025-02-01-a4d4c4e0",
        "f" * 64,
        AT,
        {"backtest_id": "a" * 32, "research_run_id": "r"},
        {"gold.features": 4},
        "e" * 64,
        {"bar": 3, "filing": 1, "release": 2},
        120,
        "/Volumes/pitlake_dev/control/releases/world-2025-02-01-a4d4c4e0.json.gz",
        "d" * 64,
        2_400_000,
        "0.1.0",
        ("bar:AAPL/2025-01-02",),
        tuple(problems),
        3.0,
    )


def test_a_release_is_described_and_recorded():
    ok = job.describe(released())
    assert ok.startswith("RELEASED: world-2025-02-01-a4d4c4e0 at 2025-02-01 06:00 UTC")
    assert "6 items (bar 3, filing 1, release 2), 120 rows, 2.4 MB" in ok
    assert "withholds 1 items as PITLake does (bar:AAPL/2025-01-02)" in ok
    bad = job.describe(released(["without bar:AAPL/2025-01-02: close of AAPL differs"]))
    assert bad.startswith("FAILED:") and "close of AAPL differs" in bad
    args = SimpleNamespace(run_id="run", code_commit="abc")
    row = job.bundle_row(args, released(), AT)
    names = re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", WORLD_BUNDLES_SCHEMA)
    assert len(row) == len(names)
    record = dict(zip(names, row, strict=True))
    assert record["verified"] is True and record["checked"] == ["bar:AAPL/2025-01-02"]


def test_micros_round_trip_with_pitlake_instants():
    from pitlake.release import micros

    assert pit_instant(micros(AT)) == AT

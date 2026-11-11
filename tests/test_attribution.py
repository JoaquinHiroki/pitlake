import re
import tomllib
from datetime import UTC, datetime, timedelta
from fractions import Fraction

import pytest
import yaml

from pitlake.attribution import (
    ALPHA,
    CHANGED,
    EVALUATED,
    FLIPPED,
    INVALID,
    NULL,
    SAME,
    SCREENED,
    Base,
    Effect,
    agent_base,
    agent_effect,
    calls_of,
    fisher,
    inputs_hash,
    null_passed,
    select,
    spread,
    strategy_base,
    strategy_effect,
    unchanged,
)
from pitlake.backtest import GRID, decision_times
from pitlake.harness import SYMBOLS, UNSETTLED, Sample, canonical, summarize
from pitlake.information import BAR, FILING, RELEASE, Item, information_set
from pitlake.jobs.attribute import (
    ATTRIBUTED,
    FAILED,
    PARTIAL,
    Attributed,
    Point,
    Progress,
    Record,
    attribution_row,
    describe,
    make_point,
    parse_args,
    point_row,
    stored_base,
    stored_points_query,
    stored_record,
    stored_rows_query,
)
from pitlake.tables import ATTRIBUTION_POINTS_SCHEMA, ATTRIBUTIONS_SCHEMA

AT = datetime(2025, 2, 3, 6, tzinfo=UTC)
RECORD = {
    "backtest_id": "b",
    "research_run_id": "r",
    "period_start": AT.date() - timedelta(days=30),
    "period_end": AT.date(),
    "input_hash": "recorded",
    "params": GRID[0].to_json(),
}
VERSIONS = {"gold.features": 4}


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def rated(**ratings: str) -> str:
    return canonical({s: ratings.get(s, "neutral") for s in SYMBOLS})


def sample(decision: str | None, calls=(("macro", "{}"),)) -> Sample:
    return Sample(decision, "h", tuple(calls), 6, None if decision else "no JSON")


def agent_point_base(*decisions: str | None) -> Base:
    samples = [sample(d) for d in decisions]
    summary = summarize([samples[: len(samples) // 2], samples[len(samples) // 2 :]], 0.8)
    return agent_base(summary, samples, 0.8, "seen")


def test_fisher_is_the_two_sided_exact_test():
    assert fisher(20, 20, 5, 10) == pytest.approx(float(Fraction(53130, 30045015)))
    assert fisher(10, 20, 5, 10) == pytest.approx(1.0)
    assert fisher(20, 20, 10, 10) == 1.0
    assert fisher(3, 3, 0, 0) == 1.0
    assert fisher(20, 20, 3, 10) < ALPHA < fisher(20, 20, 8, 10)


def test_the_strategy_attributes_each_weight_exactly():
    decision = canonical({"weights": {"AAPL": 0.1, "SPY": 0.0}, "result": "x"})
    base = strategy_base([Sample(decision, "in", (), 0), Sample(decision, "in", (), 0)])
    assert base.reproducible and base.settled == {"AAPL": 0.1, "SPY": 0.0}
    moved = strategy_effect(base, canonical({"weights": {"AAPL": 0.04, "SPY": 0.0}}))
    assert moved.outcomes == {"AAPL": CHANGED, "SPY": SAME} and moved.caused == ("AAPL",)
    assert moved.effects["AAPL"] == pytest.approx(0.06) and moved.p_values == {}
    assert not strategy_effect(base, decision).changed
    other = canonical({"weights": {"AAPL": 0.2, "SPY": 0.0}, "result": "x"})
    assert not strategy_base(
        [Sample(decision, "in", (), 0), Sample(other, "in", (), 0)]
    ).reproducible


def test_the_agent_attributes_only_settled_parts_with_their_support():
    base = agent_point_base(
        *[rated(AAPL="overweight", NVDA="overweight")] * 6, *[rated(AAPL="overweight")] * 4
    )
    assert base.settled["AAPL"] == "overweight" and "NVDA" not in base.settled
    assert base.support["AAPL"] == 10 and base.valid == 10
    flipped = agent_effect(base, [sample(rated(AAPL="underweight"))] * 10, 0.8)
    assert flipped.outcomes["AAPL"] == FLIPPED and "AAPL" in flipped.caused
    assert flipped.effects["AAPL"] == 1.0 and flipped.p_values["AAPL"] < ALPHA
    assert "NVDA" not in flipped.outcomes
    wobble = [sample(rated(AAPL="overweight"))] * 7 + [sample(rated())] * 3
    loose = agent_effect(base, wobble, 0.8)
    assert loose.outcomes["AAPL"] == UNSETTLED and loose.effects["AAPL"] == pytest.approx(0.3)
    assert loose.caused == () and loose.p_values["AAPL"] >= ALPHA
    broken = agent_effect(base, [sample(None)] * 3 + [sample(rated())] * 7, 0.8)
    assert not broken.valid and set(broken.outcomes.values()) == {INVALID}


def test_a_null_world_passes_only_when_nothing_moves():
    base = agent_point_base(*[rated(AAPL="overweight")] * 10)
    assert null_passed(agent_effect(base, [sample(rated(AAPL="overweight"))] * 10, 0.8))
    assert null_passed(unchanged(base))
    drifted = [sample(rated(AAPL="overweight"))] * 8 + [sample(rated())] * 2
    assert null_passed(agent_effect(base, drifted, 0.8))
    assert not null_passed(agent_effect(base, [sample(rated())] * 10, 0.8))
    assert not null_passed(Effect({}, {}, {"AAPL": 0.01}, ()))
    assert not null_passed(Effect({}, {}, {}, (), False))


def test_the_agent_screens_on_the_calls_its_samples_made():
    calls = calls_of([sample("a", [("macro", "{}")]), sample("b", [("snap", '{"s":1}')])])
    assert calls == (("macro", "{}"), ("snap", '{"s":1}'))
    tools = {"macro": lambda a: "CPI 3%", "snap": lambda a: f"close {a['s']}"}
    same = {"macro": lambda a: "CPI 3%", "snap": lambda a: f"close {a['s']}"}
    moved = {"macro": lambda a: "CPI 2.9%", "snap": lambda a: f"close {a['s']}"}
    assert inputs_hash(tools, calls) == inputs_hash(same, calls)
    assert inputs_hash(tools, calls) != inputs_hash(moved, calls)
    assert inputs_hash(tools, calls) == inputs_hash({**moved, "macro": tools["macro"]}, calls)


def item(kind: str, key: str, entity: str, day: int) -> Item:
    return Item(kind, key, entity, AT - timedelta(days=day), key)


def test_the_agent_tries_every_changed_filing_and_release_and_spread_bars():
    bars = [item(BAR, f"AAPL/{d}", "AAPL", d) for d in range(10, 0, -1)]
    others = [item(FILING, "0001", "AAPL", 20), item(RELEASE, "UNRATE/x", "US", 5)]
    chosen = [i.id for i in select([*bars, *others], 3)]
    assert chosen == [
        "filing:0001", "bar:AAPL/10", "bar:AAPL/6", "release:UNRATE/x", "bar:AAPL/1"
    ]  # fmt: skip
    assert [i.id for i in select(bars, 1)] == ["bar:AAPL/1"]
    assert len(select(bars, 0)) == 10 and spread([1, 2], 5) == [1, 2]


def test_arguments():
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--at", "2025-02-01 2025-02-03"])
    assert [d.isoformat() for d in args.at] == ["2025-02-01", "2025-02-03"]
    assert args.deciders == ("strategy", "agent") and args.samples == 10 and args.bars == 3
    assert args.verify == "changed" and args.minutes == 100
    assert parse_args(["--catalog", "c", "--deciders", "strategy"]).at == (None,)
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c", "--endpoint", "e", "--verify", "some"])


def strategy_point() -> Point:
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    return make_point("strategy", RECORD, None, VERSIONS, args)


def test_a_point_is_keyed_by_what_decides_it():
    args = parse_args(["--catalog", "c", "--endpoint", "e"])
    found = make_point("agent", RECORD, None, VERSIONS, args)
    assert found.at == AT and found.times == decision_times(RECORD["period_start"], AT.date())
    assert found.key == make_point("agent", RECORD, None, VERSIONS, args).key
    earlier = make_point("agent", RECORD, AT.date() - timedelta(days=1), VERSIONS, args)
    assert earlier.key != found.key
    assert make_point("agent", RECORD, None, {"gold.features": 5}, args).key != found.key
    more = parse_args(["--catalog", "c", "--endpoint", "e", "--samples", "20"])
    assert make_point("agent", RECORD, None, VERSIONS, more).key != found.key
    assert strategy_point().key != found.key and strategy_point().samples == 1


def test_records_match_their_tables_and_read_back():
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    point = strategy_point()
    decision = canonical({"weights": {"AAPL": 0.1}, "result": "x"})
    base = strategy_base([Sample(decision, "in", (), 0)] * 2)
    info = information_set([item(FILING, "0001", "AAPL", 3)], AT)
    row = dict(
        zip(columns(ATTRIBUTION_POINTS_SCHEMA), point_row(args, point, base, "w0", info, True, AT),
            strict=True)
    )  # fmt: skip
    assert row["settled"] == '{"AAPL":0.1}' and row["items"] == 1 and row["inputs_verified"]
    assert stored_base({**row, "calls": ['["macro","{}"]']}).calls == (("macro", "{}"),)
    assert stored_base(row).settled == base.settled
    effect = strategy_effect(base, canonical({"weights": {"AAPL": 0.0}}))
    found = Attributed(
        "w1", item(FILING, "0001", "AAPL", 3), EVALUATED, "other", [Sample(decision, "x", (), 0)],
        decision, effect, 2, 0, True, 0.1,
    )  # fmt: skip
    named = dict(
        zip(columns(ATTRIBUTIONS_SCHEMA), attribution_row(args, point, base, found, AT),
            strict=True)
    )  # fmt: skip
    assert named["withheld"] == ["filing:0001"] and named["kind"] == FILING
    assert named["inputs_changed"] and named["caused"] == ["AAPL"] and named["rebuilt"]
    record = stored_record(named)
    assert record == found.record(base) and record.withheld == ("filing:0001",)
    null = Attributed("w2", None, EVALUATED, "in", [], None, unchanged(base), 0, 0, None, 0.0)
    null_row = attribution_row(args, point, base, null, AT)
    named = dict(zip(columns(ATTRIBUTIONS_SCHEMA), null_row, strict=True))
    assert named["kind"] == NULL and named["withheld"] == [] and not named["inputs_changed"]
    assert "point_key IN ('a', 'b')" in stored_points_query("c", ["b", "a", "a"])
    assert "`control`.`attributions` WHERE point_key IN ('a') AND valid" in (
        stored_rows_query("c", ["a"])
    )


def progress(records, left=0, decided=True) -> Progress:
    decision = canonical({"weights": {"AAPL": 0.1}, "result": "x"})
    base = strategy_base([Sample(decision, "in", (), 0)] * 2) if decided else None
    return Progress(strategy_point(), base, 3, 1, 3, left, records)


def test_a_decision_is_attributed_once_every_world_is_done_and_the_null_is_quiet():
    base = progress({}).base
    quiet = Record((), EVALUATED, False, unchanged(base), True)
    moved = strategy_effect(base, canonical({"weights": {"AAPL": 0.0}}))
    found = {(): quiet, ("bar:AAPL/1",): Record(("bar:AAPL/1",), EVALUATED, True, moved, True)}
    assert progress(found).state == ATTRIBUTED
    assert progress(found, left=2).state == PARTIAL
    assert progress({}, decided=False).state == PARTIAL
    assert progress({(): Record((), EVALUATED, False, moved, True)}).state == FAILED
    broken = {**found, ("bar:AAPL/2",): Record(("bar:AAPL/2",), SCREENED, True, moved, False)}
    assert progress(broken).state == FAILED and "full rebuild" in progress(broken).problems[0]
    text = describe(progress(found))
    assert "null world changes nothing" in text and "bar:AAPL/1: AAPL changed +0.1000" in text


def test_the_job_runs_the_attribute_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["attribution"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "attribute" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.attribute:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    names = {p["name"] for p in job["parameters"]}
    assert names == {"backtest", "at", "deciders", "samples", "min_agreement", "bars", "verify",
                     "minutes"}  # fmt: skip
    assert job["timeout_seconds"] > 60 * float(
        next(p["default"] for p in job["parameters"] if p["name"] == "minutes")
    )

import re
import time
import tomllib
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from pitlake.attribution import (
    EVALUATED,
    FLIPPED,
    SAME,
    SAMPLED,
    agent_base,
    agent_effect,
    strategy_base,
    strategy_effect,
    unchanged,
)
from pitlake.backtest import GRID
from pitlake.harness import SYMBOLS, Sample, canonical, summarize
from pitlake.horizon import (
    FULL,
    HORIZONS,
    LADDER,
    REFINE,
    Curve,
    Measured,
    agreement,
    cutoff,
    kept_within,
    ladder,
    measure,
    screened,
    span,
    trace,
)
from pitlake.information import BAR, FILING, RELEASE, Item
from pitlake.jobs.attribute import make_point
from pitlake.jobs.suffice import Late
from pitlake.jobs.truncate import (
    BREAKS,
    FAILED,
    HOLDS,
    MEASURED,
    PARTIAL,
    Probed,
    Prober,
    Progress,
    Step,
    Tried,
    curve_rows,
    describe,
    parse_args,
    stored_curves,
    stored_curves_query,
    stored_tests_query,
    stored_tried,
    summary,
    world_row,
)
from pitlake.sufficiency import kept_hash
from pitlake.tables import HORIZON_CURVES_SCHEMA, HORIZON_TESTS_SCHEMA

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


def item(kind: str, key: str, entity: str, days: float) -> Item:
    return Item(kind, key, entity, AT - timedelta(days=days), key)


ITEMS = [
    item(BAR, "AAPL/old", "AAPL", 40),
    item(BAR, "AAPL/mid", "AAPL", 10),
    item(BAR, "AAPL/new", "AAPL", 0.5),
    item(FILING, "0001", "AAPL", 30),
    item(RELEASE, "CPIAUCSL/x", "US", 7),
]


def held(rule):
    seen = []

    def decides(days, phase):
        seen.append((days, phase))
        ok = rule(days)
        return Measured(ok, 1.0 if ok else 0.5, 1.0 if ok else 0.4, {"AAPL": SAME if ok else "x"})

    return decides, seen


def rated(**ratings: str) -> str:
    return canonical({s: ratings.get(s, "neutral") for s in SYMBOLS})


def sample(decision: str | None, model_calls: int = 6) -> Sample:
    return Sample(decision, "h", (("macro", "{}"),), model_calls, None if decision else "no JSON")


def test_a_horizon_keeps_what_became_public_after_its_cutoff():
    assert cutoff(AT, None) is None and cutoff(AT, 7) == AT - timedelta(days=7)
    assert kept_within(ITEMS, AT, None) == {i.id for i in ITEMS}
    assert kept_within(ITEMS, AT, 30) == {"bar:AAPL/mid", "bar:AAPL/new", "release:CPIAUCSL/x"}
    assert kept_within(ITEMS, AT, 7) == {"bar:AAPL/new"}
    assert kept_within(ITEMS, AT, 0) == frozenset()
    assert span(ITEMS, AT) == 41 and kept_within(ITEMS, AT, span(ITEMS, AT)) == {
        i.id for i in ITEMS
    }
    assert span([], AT) == 1
    assert ladder([7, 30, 7, 0]) == (30, 7, 0)
    for wrong in ([], [3, -1]):
        with pytest.raises(ValueError):
            ladder(wrong)


def test_the_curve_halves_the_gap_to_the_day_the_decision_stops_holding():
    decides, seen = held(lambda days: days is None or days >= 27)
    curve = trace(HORIZONS, decides, 400)
    assert curve.holds_to == 27 and curve.breaks_at == 26 and curve.recovers == ()
    assert seen[0] == (None, FULL) and [d for d, p in seen if p == LADDER] == list(HORIZONS)
    refined = [d for d, p in seen if p == REFINE]
    assert refined and len(refined) <= 4 and all(21 < d < 30 for d in refined)
    days = [d for d, _, _ in curve.points]
    assert days[0] is None and days[1:] == sorted(days[1:], reverse=True)
    assert curve.parts() == {"AAPL": 27}


def test_a_decision_that_holds_again_further_down_is_shown_to_recover():
    decides, _ = held(lambda days: days is None or days >= 27 or days == 3)
    curve = trace(HORIZONS, decides, 400)
    assert curve.recovers == (3,) and curve.parts() == {"AAPL": 27}


def test_holding_everywhere_breaking_untruncated_and_breaking_at_once():
    decides, seen = held(lambda days: True)
    curve = trace(HORIZONS, decides, 400)
    assert (curve.holds_to, curve.breaks_at) == (0, None) and len(seen) == 1 + len(HORIZONS)
    decides, seen = held(lambda days: False)
    curve = trace(HORIZONS, decides, 400)
    assert seen == [(None, FULL)] and curve.holds_to is None and not curve.full.reproduced
    decides, seen = held(lambda days: days is None or days >= 400)
    curve = trace(HORIZONS, decides, 500)
    assert (curve.holds_to, curve.breaks_at) == (400, 399)
    assert curve.parts() == {"AAPL": 400}


def test_what_a_world_measures():
    decision = canonical({"weights": {"AAPL": 0.1, "SPY": 0.0}, "result": "x"})
    base = strategy_base([Sample(decision, "in", (), 0)] * 2)
    moved = canonical({"weights": {"AAPL": 0.0, "SPY": 0.0}, "result": "y"})
    found = measure(strategy_effect(base, moved))
    assert not found.reproduced and found.same == 0.5 == found.agreement
    assert measure(strategy_effect(base, decision)) == Measured(
        True, 1.0, 1.0, {"AAPL": SAME, "SPY": SAME}
    )
    good = sample(rated(AAPL="overweight"))
    agent = agent_base(summarize([[good] * 5] * 2, 0.8), [good] * 9 + [sample(rated())], 0.8, "s")
    assert screened(agent) == pytest.approx((0.9 + 4) / 5)
    assert measure(unchanged(agent), screened(agent)).reproduced
    drawn = [good] * 4 + [sample(rated(AAPL="underweight"))] * 4 + [sample(None)] * 2
    effect = agent_effect(agent, drawn, 0.8)
    found = measure(effect, agreement(agent, drawn))
    assert not found.reproduced and found.outcomes["AAPL"] != SAME
    assert found.agreement == pytest.approx((0.4 + 4 * 0.8) / 5)
    flipped = [sample(rated(AAPL="underweight"))] * 10
    assert measure(agent_effect(agent, flipped, 0.8)).outcomes["AAPL"] == FLIPPED
    assert agreement(agent, []) == 0.0


def test_arguments():
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--horizons", "30, 7 30 0"])
    assert args.horizons == (30, 7, 0) and args.verify == "all" and args.samples == 10
    assert parse_args(["--catalog", "c", "--endpoint", "e"]).horizons == HORIZONS
    for wrong in (
        ["--deciders", "agent"],
        ["--deciders", "strategy", "--horizons", "7 -1"],
        ["--deciders", "strategy", "--horizons", ""],
        ["--deciders", "strategy", "--verify", "final"],
        ["--deciders", "strategy", "--min-agreement", "0.5"],
    ):
        with pytest.raises(SystemExit):
            parse_args(["--catalog", "c", *wrong])


def strategy_point():
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    return make_point("strategy", RECORD, None, VERSIONS, args)


def weights_base():
    decision = canonical({"weights": {"AAPL": 0.1, "SPY": 0.0}, "result": "x"})
    return strategy_base([Sample(decision, "in", (), 0)] * 2)


def tried(ok: bool, test_id: str = "t", kept: int = 3, rebuilt=True) -> Tried:
    outcomes = {"AAPL": SAME if ok else "changed", "SPY": SAME}
    m = Measured(ok, 1.0 if ok else 0.5, 1.0 if ok else 0.5, outcomes)
    return Tried(test_id, kept, m, True, EVALUATED, "in", rebuilt)


def test_records_match_their_tables_and_read_back():
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    point = strategy_point()
    found = tried(False, "t1", 2)
    test = Probed(
        7, LADDER, frozenset({"bar:AAPL/new", "filing:0001"}), found,
        [sample(rated()), sample(None, 0)], 1, rated(), unchanged(weights_base()),
        (("macro", "{}"),), 4, 1, 0.5,
    )  # fmt: skip
    row = dict(zip(columns(HORIZON_TESTS_SCHEMA), world_row(args, point, test, AT), strict=True))
    assert row["horizon_days"] == 7 and row["cutoff"] == point.at - timedelta(days=7)
    assert row["kept_count"] == 2 and row["kept_by_kind"] == {BAR: 1, FILING: 1, RELEASE: 0}
    assert row["kept_hash"] == kept_hash({"bar:AAPL/new", "filing:0001"})
    assert row["samples"] == 2 and row["invalid"] == 1 and row["errors"] == 1
    assert row["calls"] == ['["macro","{}"]'] and not row["reproduced"]
    assert stored_tried(row) == found
    curve = Curve(((None, FULL, tried(True).measured), (7, LADDER, found.measured)), None, 7)
    steps = [Step(None, FULL, tried(True, "t0", 5)), Step(7, LADDER, found, BREAKS, "a1", True)]
    rows = [
        dict(zip(columns(HORIZON_CURVES_SCHEMA), r, strict=True))
        for r in curve_rows(args, point, curve, steps, AT)
    ]
    assert [r["horizon_days"] for r in rows] == [None, 7] and rows[0]["cutoff"] is None
    assert rows[1]["boundary"] == BREAKS and rows[1]["ladder"] == " ".join(map(str, HORIZONS))
    older = [dict(r, run_id="old", holds_to=99) for r in rows]
    stored = stored_curves(older + rows[::-1])[point.key]
    assert stored.curve.breaks_at == 7 and stored.curve.holds_to is None
    assert [s.days for s in stored.steps] == [None, 7] and stored.steps[1].ablation_id == "a1"
    assert stored.curve.points[1][2] == found.measured
    assert "`control`.`horizon_tests` WHERE point_key IN ('a', 'b') AND valid" in (
        stored_tests_query("c", ["b", "a"])
    )
    assert "AND ladder = '30 7 0'" in stored_curves_query("c", ["a"], (30, 7, 0))


class Scripted(Prober):
    def __init__(self, answers, deadline=None, stored=None):
        args = parse_args(["--catalog", "c", "--deciders", "strategy"])
        late = time.monotonic() + 60 if deadline is None else deadline
        super().__init__(None, strategy_point(), weights_base(), None, ITEMS, None, args, None,
                         late, AT, stored or {})  # fmt: skip
        self.answers, self.calls = answers, []

    def test(self, kept, days, phase):
        self.calls.append(days)
        found = self.answers(days)
        if found.valid:
            self.tried[kept_hash(kept)] = found
        return found


def test_a_world_with_the_same_items_is_decided_once_and_a_late_one_stops():
    prober = Scripted(lambda days: tried(True))
    assert prober(365, LADDER).reproduced and prober(None, FULL).reproduced
    assert prober.calls == [365] and set(prober.steps) == {365, None}
    assert prober(7, LADDER) and prober.calls == [365, 7]
    stored = {kept_hash(kept_within(ITEMS, AT, 30)): tried(False)}
    late = Scripted(lambda days: tried(True), deadline=time.monotonic() - 1, stored=stored)
    assert not late(30, LADDER).reproduced and late.earlier == 1 and late.calls == []
    with pytest.raises(Late):
        late(7, LADDER)
    failing = Scripted(lambda days: Tried("t", 0, tried(False).measured, False, SAMPLED, ""))
    failing(7, LADDER)
    assert failing.tried == {}


def progress(curve=None, steps=(), base=None, broken=0) -> Progress:
    return Progress(strategy_point(), base or weights_base(), 5, 9, 2, 0, broken, curve, steps)


def measured_curve():
    ok, no = tried(True).measured, tried(False).measured
    curve = Curve(((None, FULL, ok), (30, LADDER, ok), (27, REFINE, ok), (26, REFINE, no),
                   (21, LADDER, no), (3, LADDER, ok)), 27, 26)  # fmt: skip
    steps = (
        Step(None, FULL, tried(True, kept=5)),
        Step(30, LADDER, tried(True)),
        Step(27, REFINE, tried(True), HOLDS, "a1", True),
        Step(26, REFINE, tried(False), BREAKS, "a2", True),
        Step(21, LADDER, tried(False)),
        Step(3, LADDER, tried(True)),
    )
    return curve, steps


def test_a_decision_is_measured_once_its_curve_and_boundary_are_recorded():
    curve, steps = measured_curve()
    found = progress(curve, steps)
    assert found.state == MEASURED
    text = describe(found)
    assert "reproducible down to 27 days, not at 26 days; reproducible again at 3 days" in text
    assert "holds: ablation a1 coherent" in text and "breaks: ablation a2 coherent" in text
    assert "each part holds down to (days): AAPL 27, SPY 3" in text
    assert progress().state == PARTIAL and "stopped before the curve" in describe(progress())
    incoherent = steps[:3] + (Step(26, REFINE, tried(False), BREAKS, "a2", False),)
    assert progress(curve, incoherent).state == FAILED
    assert progress(curve, steps, broken=1).state == FAILED
    untruncated = Curve(((None, FULL, tried(False).measured),), None, None)
    assert progress(untruncated, (Step(None, FULL, tried(False)),)).state == FAILED
    assert "the untruncated world decides differently" in describe(
        progress(untruncated, (Step(None, FULL, tried(False)),))
    )
    errors = Progress(strategy_point(), weights_base(), 5, 9, 2, 0, 0, errors=2)
    assert errors.state == PARTIAL and "the endpoint failed in 2 worlds" in describe(errors)
    unsettled = strategy_base([Sample(None, "in", (), 0)] * 2)
    assert progress(base=unsettled).state == FAILED


def test_the_summary_gives_the_curve_per_decider():
    curve, steps = measured_curve()
    lines = summary([progress(curve, steps), progress(curve, steps), progress()], (30, 21, 3))
    assert lines[0] == "CURVE: strategy over 2 decisions, reproducible down to 27 days, 27 days"
    assert "       full: 2 of 2 reproduced, parts same 1.00, agreement 1.00" in lines
    assert "    21 days: 0 of 2 reproduced, parts same 0.50, agreement 0.50" in lines
    assert not any("agent" in line for line in lines)


def test_the_job_runs_the_truncate_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["horizon"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "truncate" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.truncate:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    defaults = {p["name"]: p["default"] for p in job["parameters"]}
    assert set(defaults) == {
        "backtest", "at", "deciders", "samples", "min_agreement", "horizons", "verify", "minutes",
    }  # fmt: skip
    assert parse_args(["--catalog", "c", "--deciders", "strategy", "--horizons",
                       defaults["horizons"]]).horizons == HORIZONS  # fmt: skip
    assert job["timeout_seconds"] > 60 * float(defaults["minutes"])

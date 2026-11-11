import random
import re
import time
import tomllib
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from pitlake.attribution import EVALUATED, SAMPLED, SCREENED, agent_base, strategy_base, unchanged
from pitlake.backtest import GRID
from pitlake.harness import SYMBOLS, Sample, canonical, summarize
from pitlake.information import BAR, FILING, RELEASE, Item
from pitlake.jobs.attribute import make_point
from pitlake.jobs.suffice import (
    FAILED,
    PARTIAL,
    SUFFICIENT,
    Late,
    Progress,
    Search,
    Settled,
    Trial,
    Tried,
    describe,
    parse_args,
    set_row,
    stored_set,
    stored_sets_query,
    stored_tests_query,
    stored_tried,
    trial_row,
)
from pitlake.sufficiency import (
    MINIMAL,
    SEARCH,
    by_kind,
    hopeless,
    kept_hash,
    order,
    outline,
    reduce,
    split,
)
from pitlake.tables import SUFFICIENCY_TESTS_SCHEMA, SUFFICIENT_SETS_SCHEMA

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


def item(kind: str, key: str, entity: str, day: int) -> Item:
    return Item(kind, key, entity, AT - timedelta(days=day), key)


def bars(symbol: str, count: int) -> list[Item]:
    return [item(BAR, f"{symbol}/{d:02d}", symbol, d) for d in range(count, 0, -1)]


ITEMS = [
    *bars("AAPL", 8),
    *bars("SPY", 4),
    item(FILING, "0001", "AAPL", 20),
    item(RELEASE, "CPIAUCSL/x", "US", 5),
]


def rated(**ratings: str) -> str:
    return canonical({s: ratings.get(s, "neutral") for s in SYMBOLS})


def sample(decision: str | None) -> Sample:
    return Sample(decision, "h", (("macro", "{}"),), 6, None if decision else "no JSON")


def oracle(rule):
    seen = []

    def passes(kept, dropped, phase):
        seen.append((frozenset(kept), dropped, phase))
        return rule(frozenset(kept))

    return passes, seen


def test_chunks_split_by_kind_then_entity_then_time():
    kinds = split(ITEMS)
    assert [{i.kind for i in c} for c in kinds] == [{BAR}, {FILING}, {RELEASE}]
    entities = split(kinds[0])
    assert [{i.entity for i in c} for c in entities] == [{"AAPL"}, {"SPY"}]
    older, newer = split(entities[0])
    assert [i.key for i in older] == ["AAPL/08", "AAPL/07", "AAPL/06", "AAPL/05"]
    assert [i.key for i in newer] == ["AAPL/04", "AAPL/03", "AAPL/02", "AAPL/01"]


def test_the_search_finds_what_a_decision_needs():
    needed = {"bar:AAPL/01", "bar:AAPL/02", "filing:0001"}
    passes, seen = oracle(lambda kept: needed <= kept)
    assert reduce(ITEMS, passes) == needed
    assert seen[0] == (frozenset(), tuple(i.id for i in sorted(ITEMS, key=order)), SEARCH)
    assert {p for _, _, p in seen} == {SEARCH, MINIMAL}
    assert len(seen) < 2 ** len(ITEMS)


def test_a_decision_that_needs_nothing_is_found_with_one_world():
    passes, seen = oracle(lambda kept: True)
    assert reduce(ITEMS, passes) == frozenset() and len(seen) == 1
    passes, seen = oracle(lambda kept: True)
    assert reduce([], passes) == frozenset() and seen == []


def test_the_set_found_always_passes_and_no_single_item_can_go():
    ids = [i.id for i in ITEMS]
    for seed in range(40):
        chance = random.Random(seed)
        answers = {frozenset(ids): True}

        def rule(kept, answers=answers, chance=chance):
            return answers.setdefault(kept, chance.random() < 0.4)

        passes, seen = oracle(rule)
        kept = reduce(ITEMS, passes)
        assert rule(kept)
        assert all(not rule(kept - {i}) for i in kept)


def test_an_agent_test_stops_once_a_settled_part_cannot_hold():
    base = agent_base(
        summarize([[sample(rated(AAPL="overweight"))] * 5] * 2, 0.8),
        [sample(rated(AAPL="overweight"))] * 10,
        0.8,
        "seen",
    )
    agree, other = sample(rated(AAPL="overweight")), sample(rated(AAPL="underweight"))
    assert not hopeless(base, [agree, other], 10, 0.8)
    assert not hopeless(base, [other, other], 10, 0.8)
    assert hopeless(base, [other, other, other], 10, 0.8)
    assert hopeless(base, [sample(None)] * 3, 10, 0.8)
    assert not hopeless(base, [sample(None)] * 2, 10, 0.8)
    assert hopeless(base, [sample(None)], 1, 0.8)
    assert not hopeless(base, [agree] * 10, 10, 0.8)


def test_a_set_is_shown_by_entity_and_kind():
    assert by_kind(["bar:AAPL/01", "bar:SPY/01", "filing:0001"]) == {
        BAR: 2,
        FILING: 1,
        RELEASE: 0,
    }
    assert outline(ITEMS[6:9] + ITEMS[12:]) == [
        "AAPL: 2 bars 02 to 01",
        "AAPL: filing:0001",
        "SPY: 1 bar 04",
        "US: release:CPIAUCSL/x",
    ]
    assert kept_hash(["b", "a"]) == kept_hash({"a", "b"})


def test_arguments():
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--at", "2025-02-01 2025-02-03"])
    assert [d.isoformat() for d in args.at] == ["2025-02-01", "2025-02-03"]
    assert args.deciders == ("strategy", "agent") and args.samples == 10
    assert args.verify == "final" and args.minutes == 100 and args.min_agreement == 0.8
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c", "--deciders", "strategy", "--min-agreement", "0.5"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c", "--deciders", "strategy", "--verify", "changed"])


def strategy_point():
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    return make_point("strategy", RECORD, None, VERSIONS, args)


def weights_base():
    decision = canonical({"weights": {"AAPL": 0.1, "SPY": 0.0}, "result": "x"})
    return strategy_base([Sample(decision, "in", (), 0)] * 2)


class Scripted(Search):
    def __init__(self, answers, deadline=None, tried=None):
        args = parse_args(["--catalog", "c", "--deciders", "strategy"])
        base = weights_base()
        late = time.monotonic() + 60 if deadline is None else deadline
        super().__init__(None, strategy_point(), base, None, None, args, None, late, AT,
                         tried or {})  # fmt: skip
        self.answers, self.calls = answers, []

    def test(self, kept, dropped, phase):
        self.calls.append(kept)
        found = self.answers(kept)
        tried = Tried(found.passed, found.valid, found.status, found.calls, found.input_hash)
        if tried.valid:
            self.tried[kept_hash(kept)] = tried
        return tried


def test_a_search_reuses_every_world_already_tried_and_stops_when_late():
    passing = Tried(True, True, EVALUATED, (), "in")
    search = Scripted(lambda kept: passing)
    assert search(frozenset({"a"}), ("b",), SEARCH) and search(frozenset({"a"}), (), MINIMAL)
    assert len(search.calls) == 1 and search.smallest == 1 and search.tests == 1
    stored = {kept_hash({"a"}): passing}
    late = Scripted(lambda kept: passing, deadline=time.monotonic() - 1, tried=stored)
    assert late(frozenset({"a"}), (), SEARCH) and late.earlier == 1 and late.calls == []
    with pytest.raises(Late):
        late(frozenset(), ("a",), SEARCH)
    broken = Scripted(lambda kept: Tried(False, False, SAMPLED, (), ""))
    assert not broken(frozenset(), ("a",), SEARCH) and broken.tests == 0


def test_a_sampled_pass_moves_the_screen_and_a_screened_one_keeps_it():
    calls = (("symbol_snapshot", '{"symbol":"AAPL"}'),)
    answers = {
        frozenset({"a"}): Tried(True, True, SAMPLED, calls, "moved"),
        frozenset(): Tried(True, True, SCREENED, calls, "moved"),
    }
    search = Scripted(answers.__getitem__)
    assert search.screen == ((), "in")
    search(frozenset({"a"}), ("b",), SEARCH)
    assert search.screen == (calls, "moved")
    search(frozenset(), ("a",), SEARCH)
    assert search.screen == (calls, "moved") and search.sampled == 1


def test_records_match_their_tables_and_read_back():
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    point, base = strategy_point(), weights_base()
    calls = (("macro", "{}"),)
    tried = Tried(False, True, SAMPLED, calls, "h2", True)
    found = Trial(
        "t1", MINIMAL, frozenset({"bar:AAPL/01"}), ("filing:0001",), tried,
        [sample(rated()), sample(None)], True, rated(), unchanged(base), 3, 1, 0.5,
    )  # fmt: skip
    row = dict(zip(columns(SUFFICIENCY_TESTS_SCHEMA), trial_row(args, point, found, AT),
                   strict=True))  # fmt: skip
    assert row["kept_count"] == 1 and row["dropped"] == ["filing:0001"] and row["stopped"]
    assert row["samples"] == 2 and row["invalid"] == 1 and row["calls"] == ['["macro","{}"]']
    assert row["kept_hash"] == kept_hash({"bar:AAPL/01"}) and row["point_key"] == point.key
    assert stored_tried(row) == tried
    settled = Settled(("bar:AAPL/01", "filing:0001"), 9, 2, "c1", "d", True, True, "a1", True)
    named = dict(zip(columns(SUFFICIENT_SETS_SCHEMA), set_row(args, point, 14, settled, AT),
                     strict=True))  # fmt: skip
    assert named["kept_by_kind"] == {BAR: 1, FILING: 1, RELEASE: 0} and named["candidates"] == 14
    assert stored_set(named) == settled
    assert "`control`.`sufficiency_tests` WHERE point_key IN ('a', 'b') AND valid" in (
        stored_tests_query("c", ["b", "a"])
    )
    assert "`control`.`sufficient_sets` WHERE point_key IN ('a')" in stored_sets_query("c", ["a"])


def progress(settled=None, base=None, broken=0) -> Progress:
    shown = ("AAPL: 21 bars 2025-01-06 to 2025-01-26",)
    return Progress(strategy_point(), base or weights_base(), 14, 9, 2, 0, 3, broken, settled,
                    shown)  # fmt: skip


def test_a_decision_is_sufficient_once_its_set_is_confirmed_alone():
    settled = Settled(("bar:AAPL/01",), 9, 0, "c1", "d", True, True, "a1", True)
    assert progress(settled).state == SUFFICIENT
    text = describe(progress(settled))
    assert "1 are enough and none of them can go (bar 1, filing 0, release 0)" in text
    assert "the set alone decides the same" in text and "ablation a1 coherent" in text
    assert "  AAPL: 21 bars 2025-01-06 to 2025-01-26" in text
    assert progress().state == PARTIAL and "3 items still kept" in describe(progress())
    for wrong in (
        Settled((), 9, 0, "c1", "d", False, True, "a1", True),
        Settled((), 9, 0, "c1", "d", True, False, "a1", True),
        Settled((), 9, 0, "c1", "d", True, True, "a1", False),
    ):
        assert progress(wrong).state == FAILED
    assert progress(broken=1).state == FAILED
    unsettled = strategy_base([Sample(None, "in", (), 0)] * 2)
    assert progress(base=unsettled).state == FAILED


def test_the_job_runs_the_suffice_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["sufficiency"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "suffice" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.suffice:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    names = {p["name"] for p in job["parameters"]}
    assert names == {"backtest", "at", "deciders", "samples", "min_agreement", "verify", "minutes"}
    assert job["timeout_seconds"] > 60 * float(
        next(p["default"] for p in job["parameters"] if p["name"] == "minutes")
    )

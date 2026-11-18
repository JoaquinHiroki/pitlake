import re
import time
import tomllib
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from pitlake.attribution import EVALUATED, FLIPPED, SAME, SAMPLED, Base, Effect
from pitlake.backtest import GRID
from pitlake.information import BAR, FILING, RELEASE, Item
from pitlake.jobs import value as job
from pitlake.jobs.attribute import Record, make_point
from pitlake.jobs.value import (
    FAILED,
    NULL_WORLD,
    PARTIAL,
    VALUED,
    Check,
    Decided,
    Judged,
    Progress,
    describe,
    parse_args,
    source_row,
    stored_judged,
    stored_tests_query,
    value_line,
    value_point,
    value_rows,
    values_of,
)
from pitlake.tables import SOURCE_TESTS_SCHEMA, SOURCE_VALUES_SCHEMA
from pitlake.value import (
    CHANGES,
    EDGAR,
    EVERY,
    FEED,
    FRED,
    INCONCLUSIVE,
    LEVELS,
    NOTHING,
    SERIES,
    Tally,
    combine,
    judge,
    origin,
    sources,
    tally,
    value,
    withheld_hash,
)

AT = datetime(2025, 2, 3, 6, tzinfo=UTC)
RECORD = {
    "backtest_id": "b",
    "research_run_id": "r",
    "period_start": AT.date() - timedelta(days=30),
    "period_end": AT.date(),
    "input_hash": "recorded",
    "params": GRID[0].to_json(),
}
VENUES = {"AAPL": "alpaca", "MSFT": "alpaca", "BTCUSDT": "binance"}


def item(kind: str, key: str, entity: str, days: int = 1) -> Item:
    return Item(kind, key, entity, AT - timedelta(days=days), key)


ITEMS = [
    item(BAR, "AAPL/2025-01-30", "AAPL"),
    item(BAR, "MSFT/2025-01-30", "MSFT"),
    item(BAR, "BTCUSDT/2025-01-30", "BTCUSDT"),
    item(FILING, "0000320193-25-000008", "AAPL", 5),
    item(RELEASE, "UNRATE/2025-01-10", "US", 20),
    item(RELEASE, "CPIAUCSL/2025-01-15", "US", 15),
    item(RELEASE, "CPIAUCSL/2025-02-12", "US", -9),
]


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def named(schema: str, row: tuple) -> dict:
    return dict(zip(columns(schema), row, strict=True))


def effect(outcomes: dict, caused=(), p_values=None, valid=True) -> Effect:
    shifts = {k: 0.5 if v != SAME else 0.0 for k, v in outcomes.items()}
    return Effect(outcomes, shifts, p_values or {}, tuple(caused), valid)


def by_name(found):
    return {s.name: s for s in found}


def test_items_belong_to_a_feed_and_one_of_its_symbols_or_series():
    assert origin(ITEMS[0], VENUES) == ("alpaca", "alpaca/AAPL")
    assert origin(ITEMS[3], VENUES) == (EDGAR, "edgar/AAPL")
    assert origin(ITEMS[5], VENUES) == (FRED, "fred/CPIAUCSL")
    with pytest.raises(ValueError, match="no venue trades NVDA"):
        origin(item(BAR, "NVDA/2025-01-30", "NVDA"), VENUES)
    found = by_name(sources(ITEMS, VENUES))
    assert list(found) == [
        "alpaca", "binance", "edgar", "fred", "alpaca/AAPL", "alpaca/MSFT", "fred/CPIAUCSL",
        "fred/UNRATE",
    ]  # fmt: skip
    assert found["fred"].items == {i.id for i in ITEMS[4:]}
    assert found["fred/CPIAUCSL"].level == SERIES and found["fred/CPIAUCSL"].feed == FRED
    assert found["binance"].level == FEED and len(found["binance"].items) == 1
    assert found["alpaca"].withheld_hash == withheld_hash(reversed(sorted(found["alpaca"].items)))
    alone = by_name(sources(ITEMS, VENUES, [SERIES]))
    assert "binance/BTCUSDT" in alone and "edgar/AAPL" in alone and "fred" not in alone
    with pytest.raises(ValueError, match="levels are feed and/or series"):
        sources(ITEMS, VENUES, ["symbol"])


def record(item_id: str, caused=(), changed=True) -> Record:
    outcomes = {"AAPL": FLIPPED if caused else SAME}
    return Record((item_id,), SAMPLED, changed, effect(outcomes, caused), None)


def test_attribution_one_at_a_time_is_tallied_per_source():
    fred = by_name(sources(ITEMS, VENUES))["fred"]
    records = [
        record("release:UNRATE/2025-01-10", ("AAPL",)),
        record("release:CPIAUCSL/2025-01-15", changed=False),
        record("bar:AAPL/2025-01-30", ("AAPL", "MSFT")),
        Record((), SAMPLED, False, effect({"AAPL": SAME}), True),
    ]
    assert tally(records, fred) == Tally(2, 1, 1, frozenset({"AAPL"}))
    assert Tally(1, 1, 1, frozenset({"A"})) + Tally(2, 0, 0, frozenset({"B"})) == Tally(
        3, 1, 1, frozenset({"A", "B"})
    )


def test_a_source_changes_a_decision_changes_nothing_or_is_inconclusive():
    quiet = effect({"AAPL": SAME, "SPY": SAME}, p_values={"AAPL": 1.0, "SPY": 0.6})
    caused = effect({"AAPL": FLIPPED, "SPY": SAME}, ("AAPL",), {"AAPL": 0.01, "SPY": 1.0})
    weak = effect({"AAPL": FLIPPED, "SPY": SAME}, (), {"AAPL": 0.2, "SPY": 1.0})
    assert judge([quiet, quiet]) == NOTHING
    assert judge([quiet, caused]) == CHANGES
    assert judge([quiet, weak]) == INCONCLUSIVE
    assert judge([quiet, effect({"AAPL": SAME}, valid=False)]) == INCONCLUSIVE
    assert judge([]) == INCONCLUSIVE
    fred = by_name(sources(ITEMS, VENUES))["fred"]
    strategy = value("strategy", fred, [quiet, quiet], 3, Tally(3, 0, 0))
    agent = value("agent", fred, [quiet, caused], 2, Tally(2, 1, 1, frozenset({"AAPL"})))
    assert (strategy.verdict, agent.verdict) == (NOTHING, CHANGES)
    assert (agent.decisions, agent.changed, agent.parts, agent.parts_changed) == (2, 1, 4, 1)
    assert agent.largest == 0.5 and strategy.largest == 0.0
    both = combine([strategy, agent])
    assert both.decider == EVERY and both.verdict == CHANGES and both.decisions == 4
    assert both.one_at_a_time == Tally(5, 1, 1, frozenset({"AAPL"})) and both.knowable == 3
    assert combine([strategy, strategy]).verdict == NOTHING
    assert combine([strategy, value("agent", fred, [weak], 2, Tally())]).verdict == INCONCLUSIVE


def test_arguments():
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--at", "2025-02-03"])
    assert args.levels == LEVELS and args.samples == 10 and args.min_agreement == 0.8
    assert args.at == (AT.date(),) and args.minutes == 100
    assert parse_args(["--catalog", "c", "--deciders", "strategy", "--levels", "feed"]).levels == (
        FEED,
    )
    for wrong in (["--levels", "symbol"], ["--deciders", "agent"], ["--samples", "0"]):
        with pytest.raises(SystemExit):
            parse_args(["--catalog", "c", "--deciders", "strategy", *wrong])


def strategy_point():
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    return make_point("strategy", RECORD, None, {"gold.features": 4}, args)


BASE = Base('{"weights":{"AAPL":0.1}}', {"AAPL": 0.1}, {"AAPL": 1}, 1, (), "seen", True)


def test_records_match_their_tables_and_read_back():
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    point = strategy_point()
    fred = by_name(sources(ITEMS, VENUES))["fred"]
    found = Decided("t1", fred, EVALUATED, "other", [], '{"weights":{"AAPL":0.2}}',
                    effect({"AAPL": "changed"}, ("AAPL",)), 7, 2, Check(True, "a1", True),
                    1.5)  # fmt: skip
    row = named(SOURCE_TESTS_SCHEMA, source_row(args, point, BASE, found, 2, AT))
    assert row["source"] == "fred" and row["level"] == FEED and row["withheld"] == 3
    assert (
        row["knowable"] == 2
        and row["inputs_changed"]
        and row["withheld_hash"] == (fred.withheld_hash)
    )
    assert row["rows_removed"] == 7 and row["ablation_id"] == "a1" and row["coherent"]
    assert stored_judged(row) == Judged("fred", EVALUATED, True, found.effect, found.check)
    assert "`source_tests` WHERE point_key IN ('a', 'b') AND valid" in stored_tests_query(
        "c", ["b", "a"]
    )
    v = value("strategy", fred, [found.effect], 2, Tally(3, 1, 1, frozenset({"AAPL"})))
    (stored,) = value_rows(args, [v], [AT.date()], "b", AT)
    named_value = named(SOURCE_VALUES_SCHEMA, stored)
    assert named_value["verdict"] == CHANGES and named_value["parts_caused"] == ["AAPL"]
    assert named_value["decision_days"] == [AT.date()] and named_value["items_caused"] == 1
    assert value_line(v) == (
        "  fred (feed, 2 items), strategy: changes 1 of 1 decisions, 1 of 1 parts, largest "
        "effect 0.5000; one at a time 1 of 3 attributed items changed a part; CHANGES"
    )


class Script:
    def __init__(self, outcomes):
        self.outcomes, self.calls = outcomes, []

    def __call__(self, point, base, worlds, source, complete, args):
        self.calls.append(source.name)
        caused = self.outcomes.get(source.name, ())
        found = effect({"AAPL": "changed" if caused else SAME}, caused)
        return Decided(f"t-{source.name}", source, EVALUATED, "seen", [], None, found, 0, 0,
                       Check(True, "a", True), 0.1)  # fmt: skip


class Writes:
    def __init__(self):
        self.rows = {}

    def add(self, table, rows):
        self.rows.setdefault(table, []).extend(rows)

    def flush(self, force=True):
        pass


def run_point(monkeypatch, script, tested=None, deadline=None, base=BASE):
    monkeypatch.setattr(job, "decide", script)
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    loaded = type("L", (), {"items": ITEMS})()
    writer = Writes()
    late = time.monotonic() + 60 if deadline is None else deadline
    found = tuple(sources(ITEMS, VENUES, [FEED]))
    progress = value_point(strategy_point(), loaded, None, found, base, tested or {}, [], None,
                           args, writer, late, AT)  # fmt: skip
    return progress, writer


def test_a_decision_is_valued_once_the_null_world_and_every_source_are_decided(monkeypatch):
    script = Script({"alpaca": ("AAPL",)})
    found, writer = run_point(monkeypatch, script)
    assert script.calls == ["none", "alpaca", "binance", "edgar", "fred"]
    assert found.state == VALUED and found.left == 0 and found.null.effect.caused == ()
    assert found.knowable == {"alpaca": 2, "binance": 1, "edgar": 1, "fred": 2}
    assert len(writer.rows["source_tests"]) == 5
    text = describe(found)
    assert "strategy at 2025-02-03 06:00 UTC" in text and "4 sources; 5 worlds decided" in text
    assert "  null world (0 items, evaluated): same; ablation coherent" in text
    assert "  without alpaca (2 items, evaluated): CHANGED; ablation coherent; AAPL changed" in text
    again, rewritten = run_point(monkeypatch, Script({}), tested=dict(found.tested))
    assert again.state == VALUED and rewritten.rows == {}


def test_a_late_run_stops_and_a_broken_world_fails(monkeypatch):
    found, writer = run_point(monkeypatch, Script({}), deadline=time.monotonic() - 1)
    assert found.state == PARTIAL and found.left == 5 and writer.rows == {}
    nothing, _ = run_point(monkeypatch, Script({}), deadline=time.monotonic() - 1, base=None)
    assert nothing.state == PARTIAL and "no decision in the real world yet" in describe(nothing)
    broken = Judged("fred", EVALUATED, True, effect({"AAPL": SAME}), Check(False, "a2", False))
    bad = Progress(strategy_point(), BASE, (NULL_WORLD,), {"x": broken}, {})
    assert bad.state == FAILED and "the world without fred differs from the full rebuild" in (
        bad.problems
    )
    assert "the ablation a2 without fred is not coherent" in bad.problems
    moved = Judged("none", EVALUATED, True, effect({"AAPL": "changed"}, ("AAPL",)), Check(True,
                   "a3", True))  # fmt: skip
    null = Progress(strategy_point(), BASE, (NULL_WORLD,), {NULL_WORLD.withheld_hash: moved}, {})
    assert "the null world changed it" in null.problems
    shaky = Base(None, {}, {}, 0, (), "", False)
    assert Progress(strategy_point(), shaky, (NULL_WORLD,), {}, {}).state == FAILED


def test_values_per_decider_and_across_them(monkeypatch):
    strategy, _ = run_point(monkeypatch, Script({"alpaca": ("AAPL",)}))
    agent = Progress(
        type("P", (), {"decider": "agent"})(),
        BASE,
        strategy.worlds,
        dict(strategy.tested),
        strategy.knowable,
        [record("release:UNRATE/2025-01-10", ("AAPL",))],
    )
    every = values_of([strategy, agent], strategy.worlds[1:])
    found = {(v.decider, v.source.name): v for v in every}
    assert found[("strategy", "fred")].verdict == NOTHING
    assert found[("agent", "fred")].one_at_a_time.caused == 1
    assert found[(EVERY, "fred")].verdict == NOTHING and found[(EVERY, "alpaca")].verdict == CHANGES
    assert found[(EVERY, "alpaca")].decisions == 2 and len(found) == 12


def test_the_job_runs_the_value_entry_point():
    with open("resources/jobs.yml") as found:
        definition = yaml.safe_load(found)["resources"]["jobs"]["data_value"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = definition["tasks"]
    assert task["task_key"] == "value" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.value:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    defaults = {p["name"]: p["default"] for p in definition["parameters"]}
    assert set(defaults) == {
        "backtest", "at", "deciders", "samples", "min_agreement", "levels", "minutes",
    }  # fmt: skip
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--levels", defaults["levels"]])
    assert args.levels == LEVELS and definition["timeout_seconds"] > 60 * float(defaults["minutes"])

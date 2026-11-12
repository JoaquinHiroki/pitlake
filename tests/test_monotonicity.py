import re
import time
import tomllib
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from pitlake.attribution import EVALUATED, SAMPLED
from pitlake.backtest import GRID
from pitlake.harness import SYMBOLS, Sample, canonical
from pitlake.information import BAR, FILING, RELEASE, Item
from pitlake.jobs.attribute import make_point
from pitlake.jobs.suffice import Late
from pitlake.jobs.supply import (
    FAILED,
    MEASURED,
    MONOTONIC,
    NON_MONOTONIC,
    PARTIAL,
    Link,
    Probed,
    Progress,
    Supplier,
    Tried,
    closes_query,
    curve_rows,
    describe,
    pair_rows,
    parse_args,
    stored_curves,
    stored_curves_query,
    stored_tests_query,
    stored_tried,
    supply_point,
    verdict_lines,
    verdicts,
    world_row,
)
from pitlake.monotonicity import (
    LEVELS,
    Outcome,
    compare,
    flips,
    holm,
    kept_at,
    ladder,
    ladder_text,
    middle,
    outcome,
    percent,
    quality,
    regions,
    saturates,
    shuffled,
    tilt,
    verdict,
    world_quality,
)
from pitlake.sufficiency import kept_hash
from pitlake.tables import (
    MONOTONICITY_CURVES_SCHEMA,
    MONOTONICITY_PAIRS_SCHEMA,
    MONOTONICITY_TESTS_SCHEMA,
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
VERSIONS = {"gold.features": 4}
RETURNS = {"AAPL": 0.10, "BTCUSDT": -0.10, "MSFT": 0.0, "NVDA": 0.05, "SPY": -0.05}


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def named(schema: str, row: tuple) -> dict:
    return dict(zip(columns(schema), row, strict=True))


def item(kind: str, key: str, entity: str, days: float) -> Item:
    return Item(kind, key, entity, AT - timedelta(days=days), key)


ITEMS = [
    item(BAR, "AAPL/old", "AAPL", 40),
    item(BAR, "AAPL/mid", "AAPL", 10),
    item(BAR, "AAPL/new", "AAPL", 0.5),
    item(FILING, "0001", "AAPL", 30),
    item(RELEASE, "CPIAUCSL/x", "US", 7),
]


def close(symbol: str, days: float, value: float, late: float = 0.1) -> tuple:
    event = AT + timedelta(days=days)
    return (symbol, "close", event, event + timedelta(days=late), value, "bars_1d", symbol)


def closes(after: float = 31) -> list[tuple]:
    rows = []
    for symbol, change in RETURNS.items():
        rows += [close(symbol, -1, 100.0), close(symbol, 29, 100.0 * (1 + change))]
        rows.append(close(symbol, after, 1.0))
    return rows


def rated(**ratings: str) -> str:
    return canonical({s: ratings.get(s, "neutral") for s in SYMBOLS})


def weighted(**found: float) -> str:
    return canonical({"weights": {s: found.get(s, 0.0) for s in SYMBOLS}, "result": "x"})


def test_shares_and_their_nested_random_orders():
    assert ladder([1, 0.5, 0, 0.5]) == (0, 0.5, 1) and ladder_text((0, 0.25, 1.0)) == "0 0.25 1"
    for wrong in ([0.5], [-0.1, 1], [0, 1.5], []):
        with pytest.raises(ValueError):
            ladder(wrong)
    assert percent(0.1) == "10%" and percent(1.0) == "100%" and percent(0.05) == "5%"
    order = shuffled(ITEMS, "set/0")
    assert order == shuffled(reversed(ITEMS), "set/0") and sorted(order) == sorted(
        i.id for i in ITEMS
    )
    assert any(shuffled(ITEMS, f"set/{d}") != order for d in range(1, 6))
    sizes = [len(kept_at(order, f)) for f in (0, 0.2, 0.5, 1)]
    assert sizes == [0, 1, 2, 5]
    assert kept_at(order, 0.2) < kept_at(order, 0.5) < kept_at(order, 1)
    assert middle(LEVELS) == 0.5 and middle((0, 0.3, 0.7, 1)) == 0.3 and middle((0, 1)) is None


def test_the_outcome_is_the_return_from_the_instant_to_the_window_end():
    found = outcome(closes(), AT, 30)
    assert found.returns == pytest.approx(RETURNS) and found.end == AT + timedelta(days=30)
    with pytest.raises(ValueError, match="not known yet: no close of BTCUSDT"):
        outcome(closes(after=29.5), AT, 30)
    with pytest.raises(ValueError, match="no close of BTCUSDT was public"):
        outcome([r for r in closes() if r[2] > AT or r[0] != "BTCUSDT"], AT, 30)
    two = outcome([r for r in closes() if r[0] in ("AAPL", "SPY")], AT, 30)
    assert two.returns == pytest.approx({"AAPL": 0.10, "SPY": -0.05})
    with pytest.raises(ValueError, match="no symbol of the task has a close"):
        outcome([], AT, 30)


def test_quality_is_the_active_return_of_the_tilt():
    assert tilt("agent", rated(AAPL="overweight", BTCUSDT="underweight")) == {
        "AAPL": 0.2, "BTCUSDT": -0.2, "MSFT": 0.0, "NVDA": 0.0, "SPY": 0.0,
    }  # fmt: skip
    assert tilt("agent", None) == {} and tilt("strategy", weighted(AAPL=0.5))["AAPL"] == 0.5
    assert quality(tilt("agent", rated(AAPL="overweight", BTCUSDT="underweight")), RETURNS) == (
        pytest.approx(0.2 * 0.1 + 0.2 * 0.1)
    )
    assert quality(tilt("strategy", weighted(AAPL=0.2, BTCUSDT=0.2, MSFT=0.2, NVDA=0.2,
                                             SPY=0.2)), RETURNS) == pytest.approx(0)  # fmt: skip
    good, none = rated(AAPL="overweight"), None
    assert world_quality("agent", [good, none], RETURNS) == pytest.approx(0.2 * 0.1 / 2)
    assert world_quality("agent", [], RETURNS) == 0.0


def test_the_sign_flip_test_and_holm():
    assert flips([1.0, 2.0, 3.0], "s") == (1.0, 1 / 8)
    assert flips([-1.0, -2.0, -3.0], "s") == (1 / 8, 1.0)
    assert flips([0.0, 0.0], "s") == (1.0, 1.0) and flips([], "s") == (1.0, 1.0)
    many = [-1.0] * 20
    falls, rises = flips(many, "s")
    assert falls == pytest.approx(1 / 10001) and rises == 1.0 and flips(many, "s") == (falls, rises)
    assert holm([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.06, 0.06])
    assert holm([]) == []


def chains_with(dip: float, units: int = 12) -> list[dict[float, float]]:
    return [{0: 0.0, 0.5: 0.04 + 0.001 * u, 1: 0.04 - dip + 0.001 * u} for u in range(units)]


def test_a_fall_in_quality_is_a_non_monotonic_region():
    levels = (0, 0.5, 1)
    found = verdict("agent", 3, chains_with(0.03) + [{0: 0.0}], levels)
    assert found.chains == 12 and found.means[0.5] == pytest.approx(0.0455)
    pairs = {(p.low, p.high): p for p in found.pairs}
    assert pairs[(0.5, 1)].fell and pairs[(0.5, 1)].change == pytest.approx(-0.03)
    assert pairs[(0, 0.5)].rose and not pairs[(0, 1)].fell
    assert found.regions == [(0.5, 1)] and not found.monotonic and found.saturates == 0.5
    flat = verdict("agent", 3, chains_with(0.0), levels)
    assert flat.monotonic and flat.saturates == 0.5 and flat.regions == []
    assert regions(compare([{0: 0.0, 1: 0.0}], (0, 1))) == []
    assert saturates(compare([{0: 0.0, 1: 0.0}], (0, 1)), (0, 1)) == 0


def test_arguments():
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--levels", "1, 0 0.5", "--draws", "3"])
    assert args.levels == (0, 0.5, 1) and args.draws == 3 and args.days == 30
    assert args.verify == "all" and args.samples == 10
    assert parse_args(["--catalog", "c", "--deciders", "strategy"]).levels == LEVELS
    for wrong in (
        ["--deciders", "agent"],
        ["--deciders", "strategy", "--levels", "0.5"],
        ["--deciders", "strategy", "--levels", "0 2"],
        ["--deciders", "strategy", "--draws", "0"],
        ["--deciders", "strategy", "--days", "0"],
        ["--deciders", "strategy", "--verify", "boundary"],
    ):
        with pytest.raises(SystemExit):
            parse_args(["--catalog", "c", *wrong])


def strategy_point():
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    return make_point("strategy", RECORD, None, VERSIONS, args)


def tried(valid: bool = True) -> Tried:
    return Tried("t", 3, (weighted(AAPL=1.0),), valid, EVALUATED, "in", True)


def test_records_match_their_tables_and_read_back():
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--run-id", "run"])
    point = make_point("agent", RECORD, None, VERSIONS, args)
    found = Tried("t1", 2, (rated(AAPL="overweight"), None), True, SAMPLED, "in", None)
    kept = frozenset({"bar:AAPL/new", "filing:0001"})
    samples = [Sample(rated(AAPL="overweight"), "h", (), 6), Sample(None, "h", (), 4, "no JSON")]
    test = Probed(0.4, 2, kept, found, samples, 0, 4, 1, 0.5)
    row = named(MONOTONICITY_TESTS_SCHEMA, world_row(args, point, test, RETURNS, AT))
    assert row["fraction"] == 0.4 and row["draw"] == 2 and row["kept_hash"] == kept_hash(kept)
    assert row["kept_by_kind"] == {BAR: 1, FILING: 1, RELEASE: 0} and row["invalid"] == 1
    assert row["decision"] == rated(AAPL="overweight") and row["outcome_days"] == 30
    assert row["quality"] == pytest.approx(0.2 * 0.1 / 2)
    assert stored_tried(row) == found
    scored = Outcome(RETURNS, AT, AT + timedelta(days=30))
    links = [Link(0, 0.0, "t0", 0, 0.0, True), Link(0, 1.0, "t1", 5, 0.1, True)]
    rows = [
        named(MONOTONICITY_CURVES_SCHEMA, r)
        for r in curve_rows(args, point, scored, links, "a1", True, AT)
    ]
    assert rows[0]["ladder"] == ladder_text(LEVELS) and rows[1]["returns"] == RETURNS
    assert rows[1]["outcome_end"] == AT + timedelta(days=30) and rows[1]["ablation_id"] == "a1"
    older = [dict(r, run_id="old", quality=9.0) for r in rows]
    stored = stored_curves(older + rows[::-1])[point.key]
    assert stored.links == tuple(links) and stored.ablation_id == "a1" and stored.coherent
    assert stored.outcome.returns == RETURNS and stored.outcome.start == point.at
    assert "`monotonicity_tests` WHERE point_key IN ('a', 'b') AND valid" in (
        stored_tests_query("c", ["b", "a"])
    )
    query = stored_curves_query("c", ["a"], args)
    assert f"ladder = '{ladder_text(LEVELS)}' AND draws = 5 AND outcome_days = 30" in query
    assert "WHERE feature = 'close' AND entity IN ('BTCUSDT', 'SPY'" in closes_query("c")


class Scripted(Supplier):
    def __init__(self, answers, deadline=None, stored=None):
        args = parse_args(["--catalog", "c", "--deciders", "strategy"])
        late = time.monotonic() + 60 if deadline is None else deadline
        super().__init__(None, strategy_point(), None, None, args, None, late, AT,
                         stored or {}, RETURNS)  # fmt: skip
        self.answers, self.calls = answers, []

    def test(self, kept, fraction, drawn):
        self.calls.append((fraction, drawn))
        found = self.answers(fraction)
        if found.valid:
            self.tried[kept_hash(kept)] = found
        return found


def test_a_world_with_the_same_items_is_decided_once_and_a_late_one_stops():
    supplier = Scripted(lambda f: tried())
    everything = frozenset(i.id for i in ITEMS)
    assert supplier(1.0, 0, everything) == supplier(1.0, 3, everything)
    assert supplier.calls == [(1.0, 0)]
    failing = Scripted(lambda f: tried(valid=False))
    failing(0.5, 0, everything)
    failing(0.5, 1, everything)
    assert failing.calls == [(0.5, 0)] and failing.tried == {} and failing.errors == 1
    stored = {kept_hash(everything): tried()}
    late = Scripted(lambda f: tried(), deadline=time.monotonic() - 1, stored=stored)
    assert late(1.0, 0, everything).valid and late.earlier == 1 and late.calls == []
    with pytest.raises(Late):
        late(0.0, 0, frozenset())


def measured(links, ablation="a1", coherent=True, broken=0) -> Progress:
    scored = Outcome(RETURNS, AT, AT + timedelta(days=30))
    return Progress(strategy_point(), 5, 9, 2, 0, broken, scored, None, tuple(links), ablation,
                    coherent)  # fmt: skip


def chain_links(draw: int, qualities: dict[float, float]) -> list[Link]:
    return [Link(draw, f, f"t{draw}{f}", round(5 * f), q, True) for f, q in qualities.items()]


def test_a_decision_is_measured_once_every_world_is_decided_and_its_ablation_recorded():
    links = chain_links(0, {0: 0.0, 0.5: 0.02, 1: 0.01}) + chain_links(
        1, {0: 0.0, 0.5: 0.0, 1: 0.01}
    )
    found = measured(links)
    assert found.state == MEASURED and found.chains()[1] == {0: 0.0, 0.5: 0.0, 1: 0.01}
    text = describe(found, (0, 0.5, 1))
    assert "outcome to 2025-03-05 06:00 UTC: AAPL +10.00%, BTCUSDT -10.00%" in text
    assert "        50%        2  +0.0100 +0.0000 +0.0200" in text
    assert "checked: ablation a1 coherent" in text
    assert measured(links, coherent=False).state == FAILED
    assert measured(links, broken=1).state == FAILED
    left = Progress(strategy_point(), 5, 3, 0, 0, 0, errors=2)
    assert left.state == PARTIAL and "the endpoint failed in 2 worlds" in describe(left, LEVELS)
    unknown = Progress(strategy_point(), 5, 0, 0, 0, 0, unknown="the outcome is not known yet")
    assert unknown.state == FAILED and "problem: the outcome is not known yet" in describe(
        unknown, LEVELS
    )


def test_a_decision_with_no_outcome_yet_or_a_stored_curve_decides_nothing():
    args = parse_args(["--catalog", "c", "--deciders", "strategy"])
    loaded = type("L", (), {"items": ITEMS})()
    point = strategy_point()
    found = supply_point(None, point, loaded, None, {}, None, closes(after=29.5), None, args,
                         None, time.monotonic() + 60, AT)  # fmt: skip
    assert found.state == FAILED and "not known yet" in found.problems[0]
    scored, links = Outcome(RETURNS, point.at, AT), chain_links(0, {0: 0.0, 1: 0.1})
    rows = curve_rows(args, point, scored, links, None, None, AT)
    stored = stored_curves([named(MONOTONICITY_CURVES_SCHEMA, r) for r in rows])[point.key]
    again = supply_point(None, point, loaded, None, {}, stored, [], None, args, None, 0, AT)
    assert again.state == MEASURED and again.chains() == [{0: 0.0, 1: 0.1}]


def test_the_verdict_per_decider_waits_for_every_decision():
    levels = (0, 0.5, 1)
    links = [x for u in range(12) for x in chain_links(u, chains_with(0.03)[u])]
    found = [measured(links), Progress(strategy_point(), 5, 0, 0, 0, 0)]
    assert verdicts(found, levels) == []
    (result,) = verdicts(found[:1], levels)
    lines = verdict_lines(result, 30)
    assert lines[0].startswith("QUALITY: strategy over 1 decisions, 12 chains")
    assert f"  {NON_MONOTONIC} at 0.05 (Holm over 3 pairs): quality falls within 50% to 100%" in (
        lines
    )
    assert any(line.startswith("  stops helping at 50%") for line in lines)
    flat = [x for u in range(12) for x in chain_links(u, chains_with(0.0)[u])]
    (calm,) = verdicts([measured(flat)], levels)
    assert any(line.startswith(f"  {MONOTONIC} at 0.05") for line in verdict_lines(calm, 30))
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--run-id", "run"])
    rows = [named(MONOTONICITY_PAIRS_SCHEMA, r) for r in pair_rows(args, result, AT)]
    assert [(r["low"], r["high"]) for r in rows] == [(0, 0.5), (0, 1), (0.5, 1)]
    assert rows[2]["falls"] and rows[2]["change"] == pytest.approx(-0.03)


def test_the_job_runs_the_supply_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["monotonicity"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "supply" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.supply:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    defaults = {p["name"]: p["default"] for p in job["parameters"]}
    assert set(defaults) == {
        "backtest", "at", "deciders", "samples", "levels", "draws", "days", "verify", "minutes",
    }  # fmt: skip
    args = parse_args(["--catalog", "c", "--deciders", "strategy", "--levels", defaults["levels"]])
    assert args.levels == LEVELS and int(defaults["draws"]) == args.draws
    assert job["timeout_seconds"] > 60 * float(defaults["minutes"])

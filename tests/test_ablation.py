import re
from datetime import UTC, date, datetime, timedelta

import pytest

from pitlake.ablation import (
    apply,
    differences,
    hidden,
    owned_left,
    plan,
    sample_items,
    without_bars,
)
from pitlake.information import FILING_SOURCE, PRICE_SOURCE, Accounting, Item, information_set
from pitlake.jobs.ablation import (
    ADDED,
    REMOVED,
    Outcome,
    ablation_row,
    change_rows,
    describe,
    parse_args,
)
from pitlake.jobs.information_set import Loaded
from pitlake.tables import ABLATION_CHANGES_SCHEMA, ABLATIONS_SCHEMA

FIRST = date(2025, 1, 1)
AT = datetime(2025, 2, 3, 6, tzinfo=UTC)


def day(i: int) -> str:
    return (FIRST + timedelta(days=i)).isoformat()


def closed(i: int) -> datetime:
    return datetime.combine(FIRST + timedelta(days=i + 1), datetime.min.time(), UTC)


def price(feature: str, i: int) -> tuple:
    return ("AAPL", feature, closed(i), closed(i), float(i), PRICE_SOURCE, day(i))


DAYS = {"AAPL": [day(i) for i in range(30)]}
ROWS = [
    *(price("close", i) for i in range(30)),
    *(price("volatility_20d", i) for i in range(20, 30)),
    ("AAPL", "net_income_q", AT, AT - timedelta(days=5), 1.0, FILING_SOURCE, "a-10q"),
]


def test_withholding_a_bar_removes_every_row_whose_window_read_it():
    removal = plan(ROWS, ["bar:AAPL/" + day(5)], DAYS)
    removed = sorted((r[1], r[6]) for r in removal.removed)
    assert removed == sorted(
        [("close", day(i)) for i in range(5, 26)]
        + [("volatility_20d", day(i)) for i in range(20, 26)]
    )
    assert set(removal.recompute["AAPL"]) == {
        *(("close", day(i)) for i in range(6, 26)),
        *(("volatility_20d", day(i)) for i in range(20, 26)),
    }
    left = without_bars(DAYS, removal.withheld)["AAPL"]
    assert day(5) not in left and len(left) == 29
    assert removal.inputs["AAPL"] == tuple(left[:25])


def test_the_recomputation_window_starts_21_sessions_before_the_first_row():
    removal = plan(ROWS, ["bar:AAPL/" + day(28)], DAYS)
    left = without_bars(DAYS, removal.withheld)["AAPL"]
    assert removal.inputs["AAPL"] == tuple(left[28 - 21 : 29])


def test_a_filing_is_only_its_own_rows():
    removal = plan(ROWS, ["filing:a-10q"], DAYS)
    assert removal.removed == (ROWS[-1],) and removal.recompute == {} and removal.inputs == {}
    assert hidden(ROWS, ["filing:a-10q"]).removed == removal.removed


def test_hiding_a_bar_only_drops_its_own_rows():
    assert hidden(ROWS, ["bar:AAPL/" + day(5)]).removed == (ROWS[5],)


def test_applying_a_removal_keeps_everything_else():
    removal = plan(ROWS, ["filing:a-10q"], DAYS)
    world = apply(ROWS, removal, [])
    assert world == ROWS[:-1] and owned_left(world, removal.withheld) == 0
    assert owned_left(ROWS, removal.withheld) == 1
    redone = (*ROWS[0][:4], 9.0, *ROWS[0][5:])
    assert apply(ROWS, plan(ROWS, [], DAYS), [redone])[-1] == redone


def test_differences_count_both_sides():
    assert differences(ROWS, list(reversed(ROWS)))[:2] == (0, 0)
    extra, missing, sample = differences(ROWS[1:] + [ROWS[1]], ROWS)
    assert (extra, missing) == (1, 1)
    assert [side for side, _ in sample] == ["extra", "missing"]


def item(kind: str, key: str, hour: int) -> Item:
    return Item(kind, key, "AAPL", datetime(2025, 1, 1, hour, tzinfo=UTC), "d")


def test_samples_are_read_items_spread_over_time():
    items = [item("bar", f"AAPL/{day(i)}", i % 24) for i in range(9)] + [
        item("filing", "a", 1),
        item("filing", "b", 2),
        item("release", "CPIAUCSL/2025-01-15", 3),
    ]
    read = frozenset(i.id for i in items if i.key != "b")
    chosen = sample_items(items, read, 3)
    assert chosen == [
        f"bar:AAPL/{day(0)}",
        f"bar:AAPL/{day(4)}",
        f"bar:AAPL/{day(8)}",
        "filing:a",
        "release:CPIAUCSL/2025-01-15",
    ]
    assert sample_items(items, read, 1)[0] == f"bar:AAPL/{day(4)}"
    assert sample_items(items, read, 0) == []


def test_job_arguments():
    args = parse_args(["--catalog", "pitlake_prod", "--run-id", "7"])
    assert (args.withhold, args.sample, args.mode) == ((), 0, "coherent")
    args = parse_args(
        ["--catalog", "pitlake_prod", "--withhold", " filing:a-1  bar:AAPL/2025-01-02 filing:a-1",
         "--sample", "5", "--mode", "hide"]
    )  # fmt: skip
    assert args.withhold == ("filing:a-1", "bar:AAPL/2025-01-02")
    assert (args.sample, args.mode) == (5, "hide")
    with pytest.raises(ValueError):
        parse_args(["--catalog", "pitlake_prod", "--withhold", "filing:a;DROP TABLE"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_prod", "--mode", "filter"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_prod", "--sample", "-1"])


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def outcome(withheld: tuple[str, ...], input_hash: str) -> Outcome:
    info = information_set([], AT)
    accounting = Accounting(frozenset(), {}, 0, 0, ())
    return Outcome(
        "a1",
        withheld,
        (ROWS[-1],),
        (),
        len(ROWS) - 1,
        0,
        0,
        (),
        0,
        accounting,
        info.hash,
        input_hash,
        (0.5, 12.0, 8.0),
    )


def test_an_ablation_is_coherent_only_if_it_rebuilds_and_the_null_world_is_unchanged():
    assert outcome(("filing:a-10q",), "changed").coherent("recorded")
    assert outcome((), "recorded").coherent("recorded")
    assert not outcome((), "changed").coherent("recorded")
    assert "nothing (null world)" in describe(outcome((), "recorded"), "recorded")


def test_records_match_their_tables():
    args = parse_args(["--catalog", "pitlake_prod", "--run-id", "7", "--withhold", "filing:a-10q"])
    loaded = Loaded({"gold.features": 4}, None, None, None, [], [AT], ROWS)
    record = {
        "backtest_id": "b",
        "research_run_id": "r",
        "period_start": date(2025, 2, 1),
        "period_end": date(2025, 2, 3),
        "input_hash": "recorded",
    }
    found = outcome(("filing:a-10q",), "changed")
    row = ablation_row(args, found, [record, {**record, "backtest_id": "c"}], loaded, "base", AT)
    named = dict(zip(columns(ABLATIONS_SCHEMA), row, strict=True))
    assert named["backtest_ids"] == ["b", "c"] and named["withheld"] == ["filing:a-10q"]
    assert (named["inputs_changed"], named["coherent"], named["rows_removed"]) == (True, True, 1)
    assert (named["removal_seconds"], named["rebuild_seconds"]) == (0.5, 8.0)
    changes = change_rows(found)
    assert [c[1] for c in changes] == [REMOVED] and ADDED == "added"
    assert (
        dict(zip(columns(ABLATION_CHANGES_SCHEMA), changes[0], strict=True))["source_ref"]
        == "a-10q"
    )

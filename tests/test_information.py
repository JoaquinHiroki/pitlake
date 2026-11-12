import re
import tomllib
from datetime import UTC, date, datetime, timedelta

import pytest
import yaml

from pitlake.information import (
    BAR,
    FILING,
    FILING_SOURCE,
    PRICE_SOURCE,
    READ,
    RELEASE,
    RELEASE_SOURCE,
    UNREAD,
    Item,
    Reach,
    account,
    bar_items,
    filing_items,
    information_set,
    item_id,
    lineage,
    owner,
    parse_item_id,
    release_items,
    sessions,
    world,
)
from pitlake.jobs.information_set import (
    REACH,
    Decision,
    decisions_query,
    gold_commit_query,
    group_decisions,
    item_rows,
    parse_args,
    set_row,
    version_query,
)
from pitlake.tables import INFORMATION_ITEMS_SCHEMA, INFORMATION_SETS_SCHEMA, counterfactual_ddls

FIRST = date(2025, 1, 1)
DAYS = 25
SEEN = ("close", "volatility_20d", "net_income_q", "cpi_yoy")
SCOPE = Reach(("AAPL", "US"), SEEN)
ACCEPTED = datetime(2025, 1, 31, 21, 30, tzinfo=UTC)
CPI_RELEASED = datetime(2025, 1, 16, 5, tzinfo=UTC)
AT = datetime(2025, 2, 3, 6, tzinfo=UTC)


def day(i: int) -> str:
    return (FIRST + timedelta(days=i)).isoformat()


def closed(i: int) -> datetime:
    return datetime.combine(FIRST + timedelta(days=i + 1), datetime.min.time(), UTC)


BARS = [
    ("alpaca", "AAPL", day(i), "1", "2", "0.5", str(100 + i), "10", closed(i)) for i in range(DAYS)
]
FUNDAMENTALS = [
    ("AAPL", "a-10q", "net_income", "quarter", "2024-09-29", "2024-12-28", "36.33", "USD",
     "NetIncomeLoss", "10-Q", "2025-01-31", ACCEPTED),
    ("AAPL", "a-10q", "revenue", "quarter", "2024-09-29", "2024-12-28", "124.3", "USD",
     "Revenues", "10-Q", "2025-01-31", ACCEPTED),
    ("AAPL", "a-10k", "revenue", "annual", "2023-10-01", "2024-09-28", "391", "USD",
     "Revenues", "10-K", "2024-11-01", datetime(2024, 11, 2, 4, tzinfo=UTC)),
    ("MSFT", "m-10q", "net_income", "quarter", "2024-10-01", "2024-12-31", "24", "USD",
     "NetIncomeLoss", "10-Q", "2025-01-29", datetime(2025, 1, 29, 21, tzinfo=UTC)),
]  # fmt: skip


def level(feature: str, month: tuple, value: float, vintage: str) -> tuple:
    observed = datetime(*month, 1, tzinfo=UTC)
    return ("US", feature, observed, CPI_RELEASED, value, RELEASE_SOURCE, vintage)


LEVELS = [
    level("cpi", (2023, 12), 300.0, "2025-01-15"),
    level("cpi", (2024, 12), 309.0, "2025-01-15"),
    level("unrate", (2024, 12), 4.1, "2025-01-10"),
]


def price(feature: str, i: int, value: float) -> tuple:
    return ("AAPL", feature, closed(i) - timedelta(days=1), closed(i), value, PRICE_SOURCE, day(i))


ROWS = [
    *(price("close", i, 100.0 + i) for i in range(DAYS)),
    *(price("volatility_20d", i, 0.01 * i) for i in range(20, DAYS)),
    ("AAPL", "net_income_q", datetime(2024, 12, 28, tzinfo=UTC), ACCEPTED, 36.33,
     FILING_SOURCE, "a-10q"),
    ("US", "cpi_yoy", datetime(2024, 12, 1, tzinfo=UTC), CPI_RELEASED, 0.03, RELEASE_SOURCE,
     "2025-01-15"),
]  # fmt: skip


def items() -> list[Item]:
    return bar_items(BARS, SCOPE) + filing_items(FUNDAMENTALS, SCOPE) + release_items(LEVELS, SCOPE)


def received(*rows: tuple, at: datetime = AT) -> list[tuple]:
    return [(r[0], at, r[1], r[4], r[2], r[5], r[6]) for r in rows]


LATEST = received(price("close", 24, 124.0), price("volatility_20d", 24, 0.24), *ROWS[-2:])


def test_every_gold_feature_row_has_one_owning_item():
    assert owner("AAPL", "close", PRICE_SOURCE, "2025-01-02") == "bar:AAPL/2025-01-02"
    assert owner("AAPL", "net_income_q", FILING_SOURCE, "a-10q") == "filing:a-10q"
    assert owner("US", "cpi_yoy", RELEASE_SOURCE, "2025-01-15") == "release:CPIAUCSL/2025-01-15"
    assert owner("US", "unrate", RELEASE_SOURCE, "2025-01-10") == "release:UNRATE/2025-01-10"
    with pytest.raises(ValueError, match="no item owns"):
        owner("AAPL", "close", "somewhere", "x")
    with pytest.raises(ValueError):
        item_id("tweet", "x")


def test_item_ids_are_checked():
    assert parse_item_id(" filing:0000320193-25-000008 ") == "filing:0000320193-25-000008"
    assert parse_item_id("release:CPIAUCSL/2025-01-15") == "release:CPIAUCSL/2025-01-15"
    for bad in ("filing", "bar:AAPL/2025-01-02'; DROP", "news:x", "bar:a/b/c"):
        with pytest.raises(ValueError):
            parse_item_id(bad)


def test_a_price_feature_depends_on_the_bars_it_was_computed_from():
    days = sessions(items())
    window = tuple(f"bar:AAPL/{day(i)}" for i in range(4, 25))
    assert lineage("AAPL", "close", PRICE_SOURCE, day(24), days) == window
    assert lineage("AAPL", "volatility_20d", PRICE_SOURCE, day(24), days) == window
    assert lineage("AAPL", "return_1d", PRICE_SOURCE, day(2), days) == tuple(
        f"bar:AAPL/{day(i)}" for i in range(3)
    )
    assert lineage("US", "cpi_yoy", RELEASE_SOURCE, "2025-01-15", days) == (
        "release:CPIAUCSL/2025-01-15",
    )
    with pytest.raises(ValueError, match="not a price feature"):
        lineage("AAPL", "net_income_q", PRICE_SOURCE, day(19), days)
    with pytest.raises(ValueError, match="no daily bar"):
        lineage("AAPL", "close", PRICE_SOURCE, "2030-01-01", days)


def test_items_are_enumerated_within_the_decisions_reach():
    found = {i.id: i for i in items()}
    assert sum(i.kind == BAR for i in found.values()) == DAYS
    assert "filing:a-10q" in found
    assert "filing:a-10k" not in found
    assert "filing:m-10q" not in found
    assert list(i for i in found if i.startswith(RELEASE)) == ["release:CPIAUCSL/2025-01-15"]
    assert found["filing:a-10q"].knowable_at == ACCEPTED
    assert found["release:CPIAUCSL/2025-01-15"].knowable_at == CPI_RELEASED
    assert bar_items(BARS, Reach(("AAPL",), ("net_income_q",))) == []


def test_an_items_digest_is_its_content_not_its_row_order():
    once = filing_items(FUNDAMENTALS, SCOPE)
    assert filing_items(list(reversed(FUNDAMENTALS)), SCOPE) == once
    restated = [(*FUNDAMENTALS[0][:6], "40", *FUNDAMENTALS[0][7:]), *FUNDAMENTALS[1:]]
    assert filing_items(restated, SCOPE)[0].digest != once[0].digest


def test_a_symbol_on_two_venues_cannot_be_addressed():
    with pytest.raises(ValueError, match="one venue"):
        bar_items([BARS[0], ("iex", *BARS[0][1:])], SCOPE)
    with pytest.raises(ValueError, match="reported for"):
        filing_items(
            [FUNDAMENTALS[0], ("MSFT", *FUNDAMENTALS[0][1:])], Reach(("AAPL", "MSFT"), SEEN)
        )


def test_the_set_holds_what_was_knowable_at_the_instant():
    info = information_set(items(), AT)
    assert info.by_kind() == {BAR: DAYS, FILING: 1, RELEASE: 1}
    assert info.later == 0
    early = information_set(items(), datetime(2025, 1, 20, 6, tzinfo=UTC))
    assert early.by_kind() == {BAR: 19, FILING: 0, RELEASE: 1}
    assert early.later == DAYS - 19 + 1
    assert information_set(list(reversed(items())), AT).hash == info.hash
    assert early.hash != info.hash
    with pytest.raises(ValueError, match="twice"):
        information_set([*items(), items()[0]], AT)


def test_omitting_an_item_is_explicit():
    info = information_set(items(), AT, ["filing:a-10q"])
    assert "filing:a-10q" not in info.items and info.omitted == ("filing:a-10q",)
    with pytest.raises(ValueError, match="not knowable"):
        information_set(items(), datetime(2025, 1, 20, tzinfo=UTC), ["filing:a-10q"])


def test_a_complete_set_accounts_for_every_row_and_value():
    info = information_set(items(), AT)
    assert world(ROWS, info) == ROWS
    found = account(info, ROWS, LATEST, sessions(items()))
    assert (found.unaccounted, found.sample, found.values) == (0, (), 4)
    assert found.read == {
        *(f"bar:AAPL/{day(i)}" for i in range(4, 25)),
        "filing:a-10q",
        "release:CPIAUCSL/2025-01-15",
    }
    assert found.status(f"bar:AAPL/{day(0)}") == UNREAD
    assert found.status("filing:a-10q") == READ
    assert found.owned["filing:a-10q"] == 1 and found.owned[f"bar:AAPL/{day(24)}"] == 2


def test_a_missing_item_that_was_read_is_unaccounted():
    info = information_set(items(), AT, ["filing:a-10q"])
    assert len(world(ROWS, info)) == len(ROWS) - 1
    found = account(info, ROWS, LATEST, sessions(items()))
    assert found.unaccounted == 2
    assert "comes from filing:a-10q, which is not in the set" in found.sample[0]
    assert "depends on filing:a-10q, outside the set" in found.sample[1]


def test_a_missing_item_nobody_read_is_still_unaccounted():
    info = information_set(items(), AT, [f"bar:AAPL/{day(0)}"])
    found = account(info, ROWS, LATEST, sessions(items()))
    assert found.unaccounted == 1
    assert f"comes from bar:AAPL/{day(0)}" in found.sample[0]


def test_a_row_public_before_its_source_is_unaccounted():
    early = (*ROWS[-2][:3], ACCEPTED - timedelta(days=3), *ROWS[-2][4:])
    info = information_set(items(), AT)
    found = account(info, [*ROWS[:-2], early, ROWS[-1]], LATEST, sessions(items()))
    assert found.unaccounted == 1 and "before filing:a-10q" in found.sample[0]
    orphan = ("AAPL", "close", AT, AT - timedelta(days=1), 1.0, "elsewhere", "x")
    assert account(info, [*ROWS, orphan], LATEST, sessions(items())).unaccounted == 1


def test_job_arguments():
    args = parse_args(["--catalog", "pitlake_prod", "--run-id", "7"])
    assert (args.backtest, args.omit, args.run_id) == ("", (), "7")
    args = parse_args(["--catalog", "pitlake_prod", "--backtest", " 42 ", "--omit", "filing:x-1"])
    assert (args.backtest, args.omit) == ("42", ("filing:x-1",))
    with pytest.raises(ValueError):
        parse_args(["--catalog", "pitlake_prod", "--omit", "x"])
    with pytest.raises(ValueError):
        parse_args(["--catalog", "pitlake_prod", "--backtest", "1'; DROP"])


def test_queries_read_the_recorded_versions():
    latest = decisions_query("pitlake_prod", "")
    assert "`pitlake_prod`.`control`.`backtest_runs`" in latest
    assert "ORDER BY created_at DESC LIMIT 1" in latest and "replay_of IS NULL" in latest
    assert "backtest_id = '42' OR research_run_id = '42'" in decisions_query("pitlake_prod", "42")
    assert "WHERE run_id = 'r1'" in gold_commit_query("pitlake_prod", "r1")
    assert version_query("pitlake_prod", "bars_1d", 7) == (
        "SELECT * FROM `pitlake_prod`.`gold`.`bars_1d` VERSION AS OF 7"
    )


def test_backtests_that_saw_the_same_inputs_share_one_enumeration():
    record = {
        "gold_commit_run_id": "g",
        "table_versions": {"gold.features": 4},
        "period_start": date(2025, 2, 1),
        "period_end": date(2026, 10, 8),
    }
    groups = group_decisions([record, record, {**record, "period_end": date(2026, 10, 9)}])
    assert list(map(len, groups.values())) == [2, 1]
    assert Decision("g", 4, date(2025, 2, 1), date(2026, 10, 8)) in groups
    assert REACH.entities[-1] == "US" and REACH.series == ("CPIAUCSL",)


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def test_records_match_their_tables():
    info = information_set(items(), AT)
    found = account(info, ROWS, LATEST, sessions(items()))
    args = parse_args(["--catalog", "pitlake_prod", "--run-id", "7", "--code-commit", "abc"])
    record = {
        "backtest_id": "b",
        "research_run_id": "r",
        "period_start": date(2025, 2, 1),
        "period_end": date(2025, 2, 3),
        "input_hash": "h",
    }
    row = set_row(args, "e", record, info, found, {"gold.features": 4}, len(ROWS), "h", AT)
    named = dict(zip(columns(INFORMATION_SETS_SCHEMA), row, strict=True))
    assert (named["reproduced"], named["complete"], named["unaccounted"]) == (True, True, 0)
    assert named["items"] == named["read_items"] + named["unread_items"] == 27
    assert named["set_hash"] == info.hash and named["decision_at"] == AT
    assert named["items_by_kind"] == {BAR: 25, FILING: 1, RELEASE: 1}
    assert set_row(args, "e", record, info, found, {}, 0, "other", AT)[-3] is False
    rows = item_rows(args, "e", info, found, AT)
    assert len(rows) == 27
    named = dict(zip(columns(INFORMATION_ITEMS_SCHEMA), rows[0], strict=True))
    assert named["item_id"] == f"bar:AAPL/{day(0)}" and named["status"] == UNREAD
    ddls = counterfactual_ddls("pitlake_dev")
    assert len(ddls) == 15 and all("`pitlake_dev`.`control`." in d for d in ddls)


def test_the_job_runs_the_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["counterfactual"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert list(tasks) == ["enumerate", "ablate"]
    assert tasks["ablate"]["depends_on"] == [{"task_key": "enumerate"}]
    for key, module in (("enumerate", "information_set"), ("ablate", "ablation")):
        assert tasks[key]["max_retries"] == 0
        entry = tasks[key]["python_wheel_task"]["entry_point"]
        assert scripts[entry] == f"pitlake.jobs.{module}:main"
    names = {p["name"] for p in job["parameters"]}
    assert names == {"backtest", "omit", "withhold", "sample", "mode"}

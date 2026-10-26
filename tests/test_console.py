import re
from datetime import date
from pathlib import Path

import pytest

import console

ROOT = Path(__file__).resolve().parents[1]
REGISTERED = ["binance.spot_trades", "alpaca.stock_trades"]
QUERIES = (
    console.coverage_query,
    console.daily_rows_query,
    console.freshness_query,
    console.rejection_query,
    console.cross_venue_query,
    console.gate_query,
    console.backtests_query,
    console.performance_query,
    console.registry_query,
)


def test_the_app_is_granted_exactly_the_tables_it_reads():
    bundle = (ROOT / "resources" / "console.yml").read_text()
    granted = re.findall(r"securable_full_name: \$\{var\.catalog\}\.(\S+)", bundle)
    assert sorted(granted) == sorted(console.READS)
    read = set()
    for build in QUERIES:
        read |= set(re.findall(r"`pitlake_prod`\.`(\w+)`\.`(\w+)`", build("pitlake_prod")))
    assert sorted(f"{s}.{t}" for s, t in read) == sorted(console.READS)


def test_the_console_reads_only_control_tables():
    assert all(name.startswith("control.") for name in console.READS)
    with pytest.raises(ValueError, match="may read"):
        console.table("pitlake_prod", "silver.binance_spot_trades")
    with pytest.raises(ValueError, match="invalid catalog"):
        console.coverage_query("pitlake_prod`; DROP")


def test_each_source_file_counts_once_at_its_latest_processing():
    query = console.coverage_query("pitlake_dev")
    assert "PARTITION BY silver_table, source_file ORDER BY processed_at DESC" in query
    assert ":silver_table" in console.daily_rows_query("pitlake_dev")


def test_rejection_rates_are_per_drop_rule_over_the_chosen_period():
    query = console.rejection_query("pitlake_dev")
    assert "explode(rejected_by_rule)" in query and "r.action = 'drop'" in query
    assert "PARTITION BY silver_table), :days)" in query
    assert ":days" in console.daily_rows_query("pitlake_dev")
    assert ":days" in console.cross_venue_query("pitlake_dev")


def test_the_gate_shows_its_latest_real_run():
    assert "injected_fault = 'none'" in console.gate_query("pitlake_dev")


def test_backtest_metrics_come_from_the_recorded_map():
    recorded = [("sharpe", "1.24"), ("total_return", "0.11"), ("days", "615.0")]
    assert console.headline(recorded) == {
        "sharpe": 1.24,
        "total_return": 0.11,
        "annual_volatility": None,
        "max_drawdown": None,
    }
    assert console.as_dict(None) == {} and console.as_dict({"a": 1}) == {"a": 1}


def test_ingest_parameters_match_the_job():
    params = console.ingest_parameters(
        ["alpaca.stock_trades"], date(2026, 10, 1), date(2026, 10, 3), True, 0, REGISTERED
    )
    assert params == {
        "datasets": "alpaca.stock_trades",
        "period_start": "2026-10-01",
        "period_end": "2026-10-03",
        "reload": "true",
        "max_files": "0",
    }
    jobs = (ROOT / "resources" / "jobs.yml").read_text()
    ingest = jobs[jobs.index("    ingest:") : jobs.index("    transform:")]
    for name in params:
        assert f"- name: {name}" in ingest
    unbounded = console.ingest_parameters(REGISTERED, None, None, False, 30, REGISTERED)
    assert unbounded["period_start"] == "" and unbounded["reload"] == "false"


@pytest.mark.parametrize(
    ("datasets", "start", "end", "reload", "max_files", "message"),
    [
        ([], None, None, False, 0, "at least one"),
        (["edgar.filings"], None, None, False, 0, "Not registered"),
        (REGISTERED, date(2026, 10, 3), date(2026, 10, 1), False, 0, "after the last"),
        (REGISTERED, None, None, True, 0, "needs both"),
        (REGISTERED, None, None, False, -1, "negative"),
    ],
)
def test_ingest_parameters_are_checked(datasets, start, end, reload, max_files, message):
    with pytest.raises(ValueError, match=message):
        console.ingest_parameters(datasets, start, end, reload, max_files, REGISTERED)

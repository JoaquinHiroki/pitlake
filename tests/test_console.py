import re
from datetime import date
from pathlib import Path

import pytest
import yaml

import console

ROOT = Path(__file__).resolve().parents[1]
REGISTERED = ["binance.spot_trades", "alpaca.stock_trades"]
QUERIES = tuple(build for build, _ in console.VIEWS.values())


def genie_tables() -> set[str]:
    spaces = yaml.safe_load((ROOT / "resources" / "genie.yml").read_text())
    return {
        t["identifier"].removeprefix("${var.catalog}.")
        for space in spaces["resources"]["genie_spaces"].values()
        for t in space["serialized_space"]["data_sources"]["tables"]
    }


def test_the_app_is_granted_exactly_the_tables_it_and_its_genie_spaces_read():
    bundle = (ROOT / "resources" / "console.yml").read_text()
    granted = re.findall(r"securable_full_name: \$\{var\.catalog\}\.(\S+)", bundle)
    assert len(granted) == len(set(granted))
    assert set(granted) == set(console.READS) | genie_tables()
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


def test_each_view_declares_exactly_the_parameters_its_query_uses():
    for name, (build, declared) in console.VIEWS.items():
        used = set(re.findall(r"(?<!:):([a-z_]+)", build("pitlake_dev")))
        assert used == set(declared), name
        assert set(declared.values()) <= {"STRING", "INT"}


def test_backtest_headline_metrics_are_read_from_the_recorded_map():
    query = console.backtests_query("pitlake_dev")
    for metric in console.HEADLINE_METRICS:
        assert f"try_cast(metrics['{metric}'] AS DOUBLE) AS {metric}" in query


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


def test_statement_results_become_typed_columns_and_rows():
    statement = {
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "symbol", "type_name": "STRING"},
                    {"name": "value", "type_name": "LONG"},
                    {"name": "rate", "type_name": "DOUBLE"},
                    {"name": "stale", "type_name": "BOOLEAN"},
                    {"name": "metrics", "type_name": "MAP"},
                    {"name": "day", "type_name": "DATE"},
                ]
            },
            "total_row_count": 3,
            "truncated": True,
        },
        "result": {
            "data_array": [
                ["AAPL", "85777000000", "0.0012", "false", '{"sharpe":1.2}', "2024-08-02"],
                [None, None, "NaN", "true", "not json", None],
            ]
        },
    }
    table = console.result_table(statement)
    assert [c["name"] for c in table["columns"]] == [
        "symbol",
        "value",
        "rate",
        "stale",
        "metrics",
        "day",
    ]
    assert table["rows"] == [
        ["AAPL", 85777000000, 0.0012, False, {"sharpe": 1.2}, "2024-08-02"],
        [None, None, None, True, "not json", None],
    ]
    assert table["row_count"] == 3 and table["truncated"] is True
    assert console.result_table({}) == {
        "columns": [],
        "rows": [],
        "row_count": 0,
        "truncated": False,
    }


def test_the_app_stays_within_the_platform_limit_of_20_resources():
    bundle = yaml.safe_load((ROOT / "resources" / "console.yml").read_text())
    assert len(bundle["resources"]["apps"]["console"]["resources"]) <= 20

import os
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from pitlake.datasets import DATASETS, Dataset
from pitlake.jobs.operations import parse_args
from pitlake.operations import (
    Profile,
    Staged,
    costs_query,
    drift_specs,
    expired,
    freshness_query,
    full_windows,
    judge_drift,
    judge_freshness,
    latest_freshness_query,
    maintained,
    maintenance_query,
    monitor_differs,
    monitor_settings,
    profile_query,
    query_stats_query,
    retention_issues,
    staged_folders,
    tables_query,
)
from pitlake.silver import SILVER_SPECS, spec_for_table
from pitlake.tables import (
    DRIFT_SCHEMA,
    JOB_COSTS_SCHEMA,
    QUERY_STATS_SCHEMA,
    TABLE_HEALTH_SCHEMA,
    operations_ddls,
)

DAY = 86_400


def test_costs_query_prices_usage_and_names_jobs():
    query, params = costs_query(date(2026, 9, 22))
    assert params == {"since": date(2026, 9, 22)}
    assert "system.billing.usage" in query and "system.billing.list_prices" in query
    assert "p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time" in query
    assert "max_by(name, change_time)" in query
    selected = query.split(" FROM system.billing.usage")[0]
    for column in ("usage_date", "workload", "job_id", "product", "sku_name", "unit", "quantity"):
        assert column in selected
    assert {c.split()[0] for c in JOB_COSTS_SCHEMA.split(", ")} >= {"list_usd", "unpriced"}


def test_query_stats_cover_every_column_the_table_keeps():
    query, params = query_stats_query(date(2026, 9, 26))
    assert params == {"since": date(2026, 9, 26)}
    assert "FROM system.query.history h" in query and "max_by(name, change_time)" in query
    computed = {c.split()[0] for c in QUERY_STATS_SCHEMA.split(", ")} - {"recorded_at", "run_id"}
    selected = query.split(" FROM system.query.history")[0]
    assert all(f"AS {column}" in selected for column in computed - {"day"})
    assert "AS day" in selected


def test_maintenance_and_table_queries_stay_in_the_catalog():
    query, params = maintenance_query("pitlake_prod")
    assert params == {"catalog": "pitlake_prod"} and "catalog_name = :catalog" in query
    listed = tables_query("pitlake_prod")
    assert "pitlake_prod.information_schema.tables" in listed
    assert "'bronze', 'silver', 'gold', 'control'" in listed
    with pytest.raises(ValueError):
        tables_query("bad catalog")


def test_search_index_internals_are_skipped():
    assert maintained("features")
    assert not maintained("__78b6d90b_online_index_view")
    assert not maintained("event_log_78b6d90b")


def test_only_gold_must_keep_a_year_of_history():
    year = {
        "delta.deletedFileRetentionDuration": "interval 365 days",
        "delta.logRetentionDuration": "interval 365 days",
    }
    assert retention_issues("gold", year) == []
    assert retention_issues("silver", {}) == []
    drifted = retention_issues("gold", {**year, "delta.logRetentionDuration": "interval 30 days"})
    assert drifted == ["delta.logRetentionDuration is interval 30 days, not 365 days"]
    assert len(retention_issues("gold", {})) == 2


def test_staging_folders_expire_by_their_newest_file(tmp_path):
    old = tmp_path / "run-old"
    fresh = tmp_path / "run-fresh"
    nested = tmp_path / "generator" / "run-gen"
    for folder, age in ((old, 10), (fresh, 1), (nested, 9)):
        (folder / "sub").mkdir(parents=True)
        for name in ("a.parquet", "sub/b.parquet"):
            path = folder / name
            path.write_text("x")
            stamp = 1_000 * DAY - age * DAY
            os.utime(path, (stamp, stamp))
    (fresh / "late.json").write_text("x")
    os.utime(fresh / "late.json", (1_000 * DAY, 1_000 * DAY))
    (tmp_path / "loose.txt").write_text("x")

    found = {s.path.name: s for s in staged_folders(tmp_path)}
    assert set(found) == {"run-old", "run-fresh", "run-gen"}
    assert sorted(p.name for p in expired(found.values(), 1_000 * DAY)) == ["run-gen", "run-old"]
    assert expired([Staged(tmp_path / "empty", 0.0)], 1_000 * DAY) == [tmp_path / "empty"]
    assert list(staged_folders(tmp_path / "missing")) == []


def test_ddls_and_args():
    ddls = operations_ddls("pitlake_dev")
    assert "`pitlake_dev`.`control`.`job_costs`" in ddls[0]
    assert "`pitlake_dev`.`control`.`table_health`" in ddls[1]
    assert "MAP<STRING, TIMESTAMP>" in TABLE_HEALTH_SCHEMA
    args = parse_args(["--catalog", "pitlake_dev", "--step", "costs", "--lookback-days", ""])
    assert (args.step, args.lookback_days, args.staging_days) == ("costs", 30, 7)
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--step", "vacuum"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--step", "storage", "--staging-days", "0"])


NOW = datetime(2026, 10, 22, 10, tzinfo=UTC)


def test_every_dataset_has_a_tolerance_that_fits_its_cadence():
    tolerance = {d.bronze_table: d.stale_after_days for d in DATASETS.values()}
    assert tolerance["binance_spot_trades"] == 3 and tolerance["alpaca_stock_trades"] == 6
    assert tolerance["fred_series_vintages"] == 30 and tolerance["edgar_company_facts"] == 100
    with pytest.raises(ValueError):
        Dataset(source="x", name="y", columns=("a",), stale_after_days=0)


def test_freshness_query_covers_every_silver_table_and_its_last_commit():
    query = freshness_query("pitlake_prod", DATASETS.values())
    assert query.count("max(knowable_at)") == len(DATASETS)
    assert "`pitlake_prod`.`silver`.`edgar_filing_documents`" in query
    assert "`pitlake_prod`.`control`.`silver_commits`" in query
    assert "WHERE knowable_at" not in query


def test_freshness_reads_only_rows_from_the_last_check_on():
    floor = datetime(2026, 10, 25, 23, 59, 59, 123456)
    query = freshness_query(
        "pitlake_prod", [DATASETS["binance.spot_trades"]], {"binance_spot_trades": floor}
    )
    assert "WHERE knowable_at >= TIMESTAMP'2026-10-25 23:59:59.123456'" in query
    previous = latest_freshness_query("pitlake_prod")
    assert "`pitlake_prod`.`control`.`freshness`" in previous
    assert "newest_knowable_at IS NOT NULL" in previous


def test_a_table_is_stale_past_its_tolerance_or_when_empty():
    trades = DATASETS["binance.spot_trades"]
    facts = DATASETS["edgar.company_facts"]
    bars = DATASETS["alpaca.stock_bars_1d"]
    naive = NOW.replace(tzinfo=None)
    found = {
        "binance_spot_trades": (naive - timedelta(days=3, hours=1), naive - timedelta(hours=2)),
        "edgar_company_facts": (NOW - timedelta(days=56), None),
    }
    judged = {f.silver_table: f for f in judge_freshness(found, [trades, facts, bars], NOW)}
    assert judged["binance_spot_trades"].stale
    assert round(judged["binance_spot_trades"].age_days, 2) == 3.04
    assert judged["binance_spot_trades"].committed.tzinfo == UTC
    assert not judged["edgar_company_facts"].stale
    assert judged["alpaca_stock_bars_1d"].stale and judged["alpaca_stock_bars_1d"].age_days is None


def test_freshness_is_enforced_only_when_asked():
    args = parse_args(["--catalog", "pitlake_prod", "--step", "freshness"])
    assert not args.enforce_freshness
    args = parse_args(
        ["--catalog", "pitlake_prod", "--step", "freshness", "--enforce-freshness", "true"]
    )
    assert args.enforce_freshness


def test_monitors_cover_the_tables_with_daily_or_weekly_data():
    monitors = {m.table_name: m for m in monitor_settings("pitlake_prod", SILVER_SPECS.values())}
    assert sorted(monitors) == [
        "pitlake_prod.silver.alpaca_stock_bars_1d",
        "pitlake_prod.silver.alpaca_stock_trades",
        "pitlake_prod.silver.binance_spot_trades",
        "pitlake_prod.silver.coinbase_spot_candles_1m",
        "pitlake_prod.silver.fred_series_vintages",
    ]
    trades = monitors["pitlake_prod.silver.alpaca_stock_trades"]
    assert trades.output_schema_name == "pitlake_prod.monitoring"
    assert trades.granularities == ("1 day",) and trades.slicing_exprs == ("symbol",)
    fred = monitors["pitlake_prod.silver.fred_series_vintages"]
    assert fred.granularities == ("1 week",) and fred.slicing_exprs == ("series_id",)


def test_a_monitor_is_updated_only_when_its_settings_change():
    (trades,) = monitor_settings("pitlake_dev", [spec_for_table("alpaca_stock_trades")])
    assert not monitor_differs(trades, ["1 day"], ["symbol"])
    assert monitor_differs(trades, ["1 week"], ["symbol"])
    assert monitor_differs(trades, ["1 day"], None)


def test_monitor_settings_are_checked():
    bars = spec_for_table("alpaca_stock_bars_1d")
    with pytest.raises(ValueError, match="monitor_granularity"):
        replace(bars, monitor_granularity="1 hour")
    with pytest.raises(ValueError, match="not a Silver column"):
        replace(bars, monitor_slices=("ticker",))
    with pytest.raises(ValueError, match="need a monitor_granularity"):
        replace(bars, monitor_granularity="", monitor_slices=("symbol",))
    with pytest.raises(ValueError, match="not a Silver column"):
        replace(bars, monitor_levels=("adj_close",))
    with pytest.raises(ValueError, match="need a monitor_granularity"):
        replace(bars, monitor_granularity="", monitor_categories=())


def test_the_monitors_step_needs_a_workspace_folder():
    args = parse_args(
        [
            "--catalog",
            "pitlake_dev",
            "--step",
            "monitors",
            "--assets-dir",
            "/Workspace/x/monitoring",
        ]
    )
    assert args.assets_dir == "/Workspace/x/monitoring" and args.warehouse_id == ""
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--step", "monitors"])


def day(n: int) -> datetime:
    return datetime(2026, 10, n)


def profile(n: int, column: str, *, count=None, median=None, items=None, slice_value=""):
    return Profile(
        day(n),
        "symbol" if slice_value else "",
        slice_value,
        column,
        count,
        None if items is None else len(items),
        median,
        items,
    )


def test_drift_judges_every_monitored_table_but_fred():
    judged = [s.table for s in drift_specs(SILVER_SPECS.values())]
    assert sorted(judged) == [
        "alpaca_stock_bars_1d",
        "alpaca_stock_trades",
        "binance_spot_trades",
        "coinbase_spot_candles_1m",
    ]
    query = profile_query("pitlake_prod", spec_for_table("binance_spot_trades"))
    assert "`pitlake_prod`.`monitoring`.`binance_spot_trades_profile_metrics`" in query
    assert "granularity = '1 day'" in query and "':table', 'symbol'" in query
    assert "drifted BOOLEAN" in DRIFT_SCHEMA


def test_a_window_with_few_rows_is_not_full():
    profiles = [profile(n, ":table", count=1440) for n in range(18, 22)]
    profiles += [profile(22, ":table", count=1)]
    assert full_windows(profiles)[("", "")] == [day(18), day(19), day(20), day(21)]


def test_drift_compares_the_newest_full_window_with_the_one_before():
    spec = spec_for_table("coinbase_spot_candles_1m")
    profiles = []
    for n, close, volume in ((19, 108_000.0, 40.0), (20, 110_000.0, 35.0), (21, 1.1, 500.0)):
        profiles += [
            profile(n, ":table", count=1440),
            profile(n, "close", median=close),
            profile(n, "volume", median=volume),
            profile(n, "symbol", items=("BTC-USD",)),
        ]
    profiles += [profile(22, ":table", count=1), profile(22, "close", median=1.0)]
    judged = {d.column_name: d for d in judge_drift(spec, profiles)}
    close, volume, symbol = judged["close"], judged["volume"], judged["symbol"]
    assert (close.window_start, close.baseline_start) == (day(21), day(20))
    assert close.drifted and round(close.factor) == 100_000
    assert round(volume.factor, 2) == 14.29 and volume.drifted
    assert not symbol.drifted


def test_categories_drift_when_a_value_appears_or_disappears():
    spec = spec_for_table("alpaca_stock_trades")
    profiles = [
        profile(20, ":table", count=20_000, slice_value="AAPL"),
        profile(21, ":table", count=19_000, slice_value="AAPL"),
        profile(20, "tape", items=("C",), slice_value="AAPL"),
        profile(21, "tape", items=("C", "Z"), slice_value="AAPL"),
        profile(20, "price", median=250.0, slice_value="AAPL"),
        profile(21, "price", median=251.0, slice_value="AAPL"),
    ]
    judged = {d.column_name: d for d in judge_drift(spec, profiles)}
    assert judged["tape"].slice == "symbol=AAPL" and judged["tape"].appeared == ("Z",)
    assert judged["tape"].drifted
    assert not judged["price"].drifted and "size" not in judged


def test_categories_are_judged_only_when_every_value_was_profiled():
    spec = spec_for_table("alpaca_stock_bars_1d")
    truncated = replace(profile(12, "symbol", items=("AAPL", "MSFT")), distinct_count=40)
    profiles = [
        profile(5, ":table", count=20),
        profile(12, ":table", count=20),
        profile(5, "symbol", items=("AAPL", "MSFT", "NVDA", "SPY")),
        truncated,
    ]
    assert judge_drift(spec, profiles) == []

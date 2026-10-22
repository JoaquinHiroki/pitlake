import os
from datetime import UTC, date, datetime, timedelta

import pytest

from pitlake.datasets import DATASETS, Dataset
from pitlake.jobs.operations import parse_args
from pitlake.operations import (
    Staged,
    costs_query,
    expired,
    freshness_query,
    judge_freshness,
    maintained,
    maintenance_query,
    retention_issues,
    staged_folders,
    tables_query,
)
from pitlake.tables import JOB_COSTS_SCHEMA, TABLE_HEALTH_SCHEMA, operations_ddls

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

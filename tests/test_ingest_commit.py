from datetime import UTC, datetime

import pytest

from pitlake.datasets import DATASETS
from pitlake.jobs.commit_ingest import commit_rows, latest_commits_query
from pitlake.jobs.plan_ingest import dataset_keys, registry_rows
from pitlake.tables import ingest_commits_ddl, source_registry_ddl

NOW = datetime(2026, 10, 5, 7, 0, tzinfo=UTC)
ALL = list(DATASETS.values())


def test_registry_lists_every_dataset_once():
    rows = registry_rows(ALL, NOW)
    assert [(r[0], r[1]) for r in rows] == [(d.source, d.name) for d in ALL]
    assert ("edgar", "company_facts", "edgar_company_facts", "none", "xbrl_facts") == rows[-2][:5]


def test_for_each_inputs_are_the_dataset_keys_in_registry_order():
    keys = dataset_keys(ALL)
    assert keys[0] == "binance.spot_trades" and len(keys) == len(set(keys)) == len(ALL)


def test_commit_has_one_row_per_dataset_with_this_runs_loads():
    versions = {d.key: i for i, d in enumerate(ALL)}
    rows = commit_rows(ALL, versions, {"alpaca.stock_trades": (3, 120)}, "42", NOW)
    assert len(rows) == len(ALL)
    trades = next(r for r in rows if r[3] == "stock_trades" and r[2] == "alpaca")
    assert trades == ("42", NOW, "alpaca", "stock_trades", "alpaca_stock_trades", 3, 3, 120)
    # A dataset with nothing new is still committed, at its unchanged version.
    assert next(r for r in rows if r[2] == "binance")[5:] == (0, 0, 0)


def test_commit_refuses_when_a_bronze_version_is_missing():
    versions = {d.key: 1 for d in ALL[1:]}
    with pytest.raises(RuntimeError, match="partial commit"):
        commit_rows(ALL, versions, {}, "42", NOW)


def test_downstream_reads_the_latest_commit_per_dataset():
    query = latest_commits_query("pitlake_prod")
    assert "`pitlake_prod`.`control`.`ingest_commits`" in query
    assert "PARTITION BY source, dataset ORDER BY committed_at DESC) = 1" in query


def test_control_tables_are_created_idempotently():
    assert source_registry_ddl("pitlake_dev").startswith(
        "CREATE TABLE IF NOT EXISTS `pitlake_dev`.`control`.`source_registry`"
    )
    assert "bronze_version BIGINT" in ingest_commits_ddl("pitlake_dev")

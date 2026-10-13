import re
from datetime import UTC, date, datetime

import pytest

from pitlake.backtest import GRID, STRATEGY, run
from pitlake.jobs.research import (
    FEATURES_KEY,
    ReproductionFailed,
    instant,
    originals_query,
    parse_args,
    record,
)
from pitlake.pit import FeaturesSnapshot
from pitlake.tables import (
    BACKTEST_RUNS_SCHEMA,
    backtest_runs_ddl,
    gold_ddls,
    gold_retention_ddl,
)

NOW = datetime(2026, 10, 13, 7, 0, tzinfo=UTC)
EXPERIMENT = "/Users/me/pitlake-backtests-dev"


def args(*extra):
    return parse_args(["--catalog", "pitlake_dev", "--experiment", EXPERIMENT, *extra])


def test_research_args():
    parsed = args("--run-id", "42", "--code-commit", "abc", "--end", "")
    assert (parsed.start, parsed.end, parsed.replay) == (date(2025, 2, 1), None, "")
    assert args("--start", "").start == date(2025, 2, 1)
    assert args("--replay", " 4242 ").replay == "4242"
    with pytest.raises(SystemExit):
        args("--start", "2025-03-01", "--end", "2025-02-01")
    with pytest.raises(ValueError):
        args("--replay", "x'; DROP")


def test_epoch_micros_become_exact_utc_instants():
    assert instant(1_735_711_200_000_001) == datetime(2025, 1, 1, 6, 0, 0, 1, tzinfo=UTC)


def test_replay_finds_one_backtest_or_a_whole_research_run():
    query = originals_query("pitlake_prod", "4242")
    assert "FROM `pitlake_prod`.`control`.`backtest_runs`" in query
    assert "replay_of IS NULL AND (backtest_id = '4242' OR research_run_id = '4242')" in query


def test_a_record_carries_versions_code_and_hashes():
    result = run({}, [NOW], GRID[0])
    snapshot = FeaturesSnapshot(7, 9, "1142", {"edgar_company_facts": 1})
    row = record(
        args("--run-id", "42", "--code-commit", "c7e1a37"),
        GRID[0],
        result,
        snapshot,
        (date(2025, 2, 1), date(2026, 10, 8)),
        "in",
        "mlflow-1",
        NOW,
    )
    columns = re.findall(r"(\w+) (?:MAP<[^>]*>|\w+)", BACKTEST_RUNS_SCHEMA)
    named = dict(zip(columns, row, strict=True))
    assert len(named["backtest_id"]) == 32
    assert named["research_run_id"] == "42" and named["strategy"] == STRATEGY
    assert named["table_versions"] == {FEATURES_KEY: 9}
    assert named["silver_versions"] == {"edgar_company_facts": 1}
    assert (named["code_commit"], named["input_hash"]) == ("c7e1a37", "in")
    assert named["result_hash"] == result.fingerprint
    assert named["metrics"] == result.metrics
    assert (named["mlflow_run_id"], named["replay_of"], named["reproduced"]) == (
        "mlflow-1",
        None,
        None,
    )


def test_a_failed_reproduction_keeps_its_rows():
    error = ReproductionFailed({"b1": ["result aaa -> bbb"]}, [("row",)])
    assert error.rows == [("row",)]
    assert "1 backtests did not reproduce: b1: result aaa -> bbb" in str(error)


def test_gold_versions_stay_readable_for_a_year():
    assert all(
        "'delta.deletedFileRetentionDuration' = 'interval 365 days'" in d for d in gold_ddls("x")
    )
    assert gold_retention_ddl("pitlake_prod", "features") == (
        "ALTER TABLE `pitlake_prod`.`gold`.`features` SET TBLPROPERTIES ("
        "'delta.deletedFileRetentionDuration' = 'interval 365 days', "
        "'delta.logRetentionDuration' = 'interval 365 days')"
    )
    assert "MAP<STRING, DOUBLE>" in backtest_runs_ddl("pitlake_dev")

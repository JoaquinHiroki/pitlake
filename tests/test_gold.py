from datetime import UTC, date, datetime, timedelta

import pytest

from pitlake.gold import (
    ANNUAL,
    CANDLES,
    INSTANT,
    METRICS,
    MINUTE_BAR_SOURCES,
    QUARTER,
    SILVER_INPUTS,
    Metric,
    MinuteBarSource,
    duration_of,
    duration_sql,
    feature_name,
    gold_fault,
    macro_name_sql,
    metric_rows,
    session_end_sql,
)
from pitlake.jobs import quality_gate, transform_silver
from pitlake.jobs.build_gold import (
    bar_slices,
    bars_log_rows,
    bars_predicate,
    check_rows,
    parse_args,
    pending_bar_files_query,
)
from pitlake.jobs.commit_gold import commit_rows
from pitlake.leakage import (
    BAR_CLOSED,
    COUNTERFACTUAL,
    CheckResult,
    LeakageFound,
    choose_instants,
)
from pitlake.pit import require_instant
from pitlake.quality import FAULTS, FUNDAMENTALS_DATED_BY_PERIOD, NO_FAULT, VENUE_MISSING_MINUTES
from pitlake.silver import BINANCE_SPOT_TRADES_SILVER
from pitlake.tables import GOLD_TABLES, gold_control_ddls, gold_ddls

NOW = datetime(2026, 10, 13, 7, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (None, date(2025, 3, 29), INSTANT),
        (date(2024, 12, 29), date(2025, 3, 29), QUARTER),
        (date(2024, 9, 29), date(2025, 1, 4), QUARTER),
        (date(2024, 9, 29), date(2025, 9, 27), ANNUAL),
        (date(2024, 9, 29), date(2025, 3, 29), None),
        (date(2024, 9, 29), date(2025, 6, 28), None),
    ],
)
def test_duration_tells_quarters_years_and_balances_from_year_to_date(start, end, expected):
    assert duration_of(start, end) == expected


def test_duration_sql_uses_the_same_bounds():
    sql = duration_sql()
    assert "BETWEEN 80 AND 100 THEN 'quarter'" in sql
    assert "BETWEEN 350 AND 380 THEN 'annual'" in sql
    assert "IS NULL THEN 'instant'" in sql


def test_metrics_map_concepts_in_priority_order():
    rows = metric_rows()
    revenue = [r for r in rows if r[0] == "revenue"]
    assert [r[2] for r in revenue] == [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
    ]
    assert [r[4] for r in revenue] == [1, 2, 3]
    shares = next(r for r in rows if r[0] == "shares_outstanding")
    assert shares[1:4] == ("dei", "EntityCommonStockSharesOutstanding", "shares")


def test_feature_names_say_the_period_they_cover():
    names = {f for m in METRICS for f in m.features}
    assert {"revenue_q", "revenue_fy", "eps_diluted_q", "assets", "operating_cash_flow_fy"} <= names
    assert "operating_cash_flow_q" not in names
    assert feature_name("net_income", QUARTER) == "net_income_q"


def test_metric_validation():
    with pytest.raises(ValueError, match="durations"):
        Metric("x", ("X",), "USD", ("monthly",), "d")
    with pytest.raises(ValueError, match="concept"):
        Metric("x", (), "USD", (QUARTER,), "d")
    with pytest.raises(ValueError, match="unique"):
        metric_rows((METRICS[0], METRICS[0]))


def test_macro_names_and_session_ends():
    assert "WHEN 'UNRATE' THEN 'unrate'" in macro_name_sql()
    assert "ELSE lower(series_id)" in macro_name_sql()
    end = session_end_sql()
    assert "venue IN ('alpaca')" in end
    assert "America/New_York" in end
    assert "CAST(date_add(session_date, 1) AS TIMESTAMP)" in end


def test_bar_sources_cover_both_venues_and_the_equities():
    assert [s.venue for s in MINUTE_BAR_SOURCES] == ["binance", "coinbase", "alpaca"]
    assert MINUTE_BAR_SOURCES[2].quantity == "size"
    assert {s.table for s in SILVER_INPUTS} >= {"edgar_filings", "fred_series_vintages"}
    with pytest.raises(ValueError, match="kind"):
        MinuteBarSource(BINANCE_SPOT_TRADES_SILVER, "quotes")
    assert MinuteBarSource(BINANCE_SPOT_TRADES_SILVER, CANDLES).venue == "binance"


def test_only_the_gold_fault_reaches_gold():
    assert FUNDAMENTALS_DATED_BY_PERIOD in FAULTS
    assert gold_fault(FUNDAMENTALS_DATED_BY_PERIOD) == FUNDAMENTALS_DATED_BY_PERIOD
    assert gold_fault(VENUE_MISSING_MINUTES) == NO_FAULT


def test_earlier_stages_ignore_the_gold_fault():
    silver = transform_silver.parse_args(
        ["--catalog", "pitlake_dev", "--inject-fault", FUNDAMENTALS_DATED_BY_PERIOD]
    )
    assert silver.inject_fault == NO_FAULT
    gate = quality_gate.parse_args(
        ["--catalog", "pitlake_dev", "--inject-fault", FUNDAMENTALS_DATED_BY_PERIOD]
    )
    assert gate.fault == NO_FAULT and gate.period is None


def test_build_args():
    args = parse_args(
        [
            "--catalog",
            "pitlake_dev",
            "--run-id",
            "42",
            "--inject-fault",
            FUNDAMENTALS_DATED_BY_PERIOD,
            "--period-start",
            "",
        ]
    )
    assert (args.run_id, args.fault, args.reprocess) == ("42", FUNDAMENTALS_DATED_BY_PERIOD, False)
    assert parse_args(["--catalog", "pitlake_dev", "--inject-fault", "venue_missing_minutes"]).fault
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--reprocess", "true"])


def test_pending_bar_files_skip_files_gold_already_has():
    query, params = pending_bar_files_query(
        "pitlake_dev", MINUTE_BAR_SOURCES[0], NOW, date(2025, 1, 1), None, False
    )
    assert "processed_at <= :committed_at" in query
    assert "LEFT ANTI JOIN `pitlake_dev`.`control`.`gold_bars_log`" in query
    assert "g.silver_processed_at = f.processed_at" in query
    assert params == {
        "silver_table": "binance_spot_trades",
        "committed_at": NOW,
        "period_start": date(2025, 1, 1),
        "venue": "binance",
    }
    query, params = pending_bar_files_query(
        "pitlake_dev", MINUTE_BAR_SOURCES[0], NOW, date(2025, 1, 1), date(2025, 1, 2), True
    )
    assert "ANTI JOIN" not in query
    assert "venue" not in params


def _file(symbol, period, name="f"):
    return {
        "symbol": symbol,
        "period_start": period,
        "source_file": f"{name}-{symbol}-{period}",
        "sha256": "s",
        "processed_at": NOW,
    }


def test_bar_slices_replace_each_symbol_by_its_periods():
    files = [_file("AAPL", date(2025, 1, 2)), _file("MSFT", date(2025, 1, 2))]
    files.append(_file("AAPL", date(2025, 1, 3)))
    assert bar_slices("alpaca", files) == {
        "AAPL": [date(2025, 1, 2), date(2025, 1, 3)],
        "MSFT": [date(2025, 1, 2)],
    }
    with pytest.raises(RuntimeError, match="two files"):
        bar_slices("alpaca", [*files, _file("AAPL", date(2025, 1, 2), "g")])
    assert bars_predicate("alpaca", "AAPL", [date(2025, 1, 3), date(2025, 1, 2)]) == (
        "venue = 'alpaca' AND symbol = 'AAPL' AND period IN (DATE'2025-01-02', DATE'2025-01-03')"
    )
    with pytest.raises(ValueError):
        bars_predicate("alpaca", "AAPL", [])


def test_bars_log_counts_files_without_bars_as_zero():
    files = [_file("AAPL", date(2025, 1, 2)), _file("AAPL", date(2025, 1, 3))]
    rows = bars_log_rows("alpaca", files, {"f-AAPL-2025-01-02": 390}, 7, "42", NOW)
    assert [r[7] for r in rows] == [390, 0]
    assert rows[0] == (
        "alpaca",
        "AAPL",
        date(2025, 1, 2),
        "f-AAPL-2025-01-02",
        "s",
        NOW,
        7,
        390,
        NOW,
        "42",
    )


def test_check_rows_and_leakage_failure():
    results = [
        CheckResult(BAR_CLOSED, "bars_1m", 100, 0),
        CheckResult(COUNTERFACTUAL, "2025-01-01T00:00:00", 10, 3, "[{}]"),
    ]
    assert check_rows(results, "42", NOW, NO_FAULT)[1] == (
        "42",
        NOW,
        COUNTERFACTUAL,
        "2025-01-01T00:00:00",
        10,
        3,
        "[{}]",
        NO_FAULT,
    )
    error = LeakageFound(results)
    assert error.results == [results[1]]
    assert "found 3 cases: counterfactual 2025-01-01T00:00:00: 3 cases" in str(error)
    with pytest.raises(ValueError, match="check"):
        CheckResult("guess", "x", 0, 0)


def test_instants_take_the_latest_and_some_history_just_before_they_became_knowable():
    days = [NOW - timedelta(days=d) for d in range(10)]
    chosen = choose_instants([*days, None, days[0]], latest=2, spread=2)
    just_before = timedelta(microseconds=1)
    assert chosen == sorted(
        [days[0] - just_before, days[1] - just_before, days[9] - just_before]
        + [days[5] - just_before]
    )
    assert choose_instants([], latest=2) == []
    assert choose_instants(days, latest=0) == []


@pytest.mark.parametrize("at", [None, date(2025, 1, 2), datetime(2025, 1, 2, 12)])
def test_a_feature_read_requires_an_instant(at):
    with pytest.raises(TypeError, match="instant"):
        require_instant(at)
    assert require_instant(NOW) == NOW


def test_gold_commit_needs_every_table():
    versions = {t: i for i, t in enumerate(GOLD_TABLES)}
    inputs = {t: {"binance_spot_trades": 12} for t in GOLD_TABLES}
    rows = commit_rows(versions, inputs, "42", NOW)
    assert [r[2] for r in rows] == list(GOLD_TABLES)
    assert rows[3] == ("42", NOW, "features", 3, {"binance_spot_trades": 12})
    del inputs["features"]
    with pytest.raises(RuntimeError, match="features"):
        commit_rows(versions, inputs, "42", NOW)


def test_gold_ddls():
    ddls = gold_ddls("pitlake_dev")
    assert len(ddls) == 4
    features = ddls[3]
    assert "CREATE TABLE IF NOT EXISTS `pitlake_dev`.`gold`.`features`" in features
    key = "PRIMARY KEY (entity, feature, event_time, source_ref, knowable_at TIMESERIES)"
    assert key in features
    assert "CLUSTER BY (entity, feature, knowable_at)" in features
    assert "CLUSTER BY (symbol, bar_time)" in ddls[0]
    control = gold_control_ddls("pitlake_dev")
    assert len(control) == 5
    commits = next(d for d in control if "`control`.`gold_commits`" in d)
    assert "inputs MAP<STRING, BIGINT>" in commits

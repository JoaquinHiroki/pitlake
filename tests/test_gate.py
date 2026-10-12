from datetime import UTC, date, datetime

import pytest

from pitlake.gate import (
    ANY,
    BTC,
    REJECTION_RATE,
    THRESHOLDS,
    VENUE_DIVERGENCE,
    VENUE_MISSING,
    DayFiles,
    GateFailure,
    Measurement,
    Threshold,
    days_to_compare,
    judge,
    rejection_measurements,
    threshold_for,
    venue_measurements,
)
from pitlake.jobs.plan_transform import threshold_rows
from pitlake.jobs.quality_gate import (
    day_predicate,
    log_rows,
    parse_args,
    result_rows,
    uncommitted_log_query,
)
from pitlake.tables import cross_venue_ddl, quality_waivers_ddl
from pitlake.venues import COMPARISON_VALUES

NOW = datetime(2026, 10, 13, 7, 0, tzinfo=UTC)
DAY = date(2026, 10, 12)


def test_a_specific_threshold_wins_over_the_default():
    facts = threshold_for(REJECTION_RATE, "edgar_company_facts.duplicate_key", THRESHOLDS)
    assert facts.max_rate == 1
    assert (
        threshold_for(REJECTION_RATE, "binance_spot_trades.key_present", THRESHOLDS).subject == ANY
    )
    with pytest.raises(ValueError, match="no threshold"):
        threshold_for(VENUE_MISSING, "BTC", [])


def test_threshold_validation():
    with pytest.raises(ValueError, match="check"):
        Threshold("speed", ANY, 0.1, 0, "d")
    with pytest.raises(ValueError, match="limits"):
        Threshold(VENUE_MISSING, ANY, 2, 0, "d")
    with pytest.raises(ValueError, match="say why"):
        Threshold(VENUE_MISSING, ANY, 0.1, 0, "")


def _rate(subject, measured, failed, day=DAY):
    return Measurement(REJECTION_RATE, subject, day, measured, failed)


@pytest.mark.parametrize(
    ("subject", "measured", "failed", "passed"),
    [
        ("alpaca_stock_bars_1d.ohlc_consistent", 4, 1, True),
        ("alpaca_stock_bars_1d.ohlc_consistent", 4, 4, False),
        ("fred_series_vintages.value_reported", 950, 1, True),
        ("edgar_filings.timestamp_plausible", 1001, 7, True),
        ("binance_spot_trades.price_and_quantity_positive", 3_000_000, 30_003, False),
        ("edgar_company_facts.duplicate_key", 52_000, 26_000, True),
    ],
)
def test_rejection_limits(subject, measured, failed, passed):
    (result,) = judge([_rate(subject, measured, failed)], THRESHOLDS, {})
    assert result.passed is passed and result.breach is not passed


@pytest.mark.parametrize(
    ("day", "divergent", "missing", "breaches"),
    [
        # Measured in prod, 2025-01-01 to 2026-10-06 (ADR 0007).
        (date(2025, 10, 10), 13, 0, {VENUE_DIVERGENCE}),
        (date(2025, 10, 25), 0, 361, {VENUE_MISSING}),
        (date(2026, 5, 8), 0, 402, {VENUE_MISSING}),
        (date(2026, 2, 24), 0, 7, set()),
        (date(2026, 5, 4), 0, 0, set()),
    ],
)
def test_venue_limits_catch_exactly_the_bad_days(day, divergent, missing, breaches):
    results = judge(venue_measurements(BTC, [(day, 1440, divergent, missing)]), THRESHOLDS, {})
    assert {r.check for r in results if r.breach} == breaches


def test_a_waiver_accepts_one_breach_and_keeps_its_reason():
    day = date(2025, 10, 10)
    waivers = {(VENUE_DIVERGENCE, "BTC", day): "Crash of 2025-10-10"}
    results = judge(venue_measurements(BTC, [(day, 1440, 13, 500)]), THRESHOLDS, waivers)
    divergence, missing = results
    assert (divergence.passed, divergence.waived, divergence.breach) == (False, True, False)
    assert divergence.waiver_reason == "Crash of 2025-10-10"
    assert missing.breach
    message = str(GateFailure([missing]))
    assert "venue_missing BTC 2025-10-10: 500 of 1440" in message


def test_rejections_are_summed_per_table_rule_and_day():
    rows = [
        ("fred_series_vintages", DAY, 950, {"value_reported": 1}),
        ("fred_series_vintages", DAY, 940, {"value_reported": 2}),
        ("binance_spot_trades", DAY, 3_000_000, {}),
    ]
    assert rejection_measurements(rows) == [_rate("fred_series_vintages.value_reported", 1890, 3)]


def _files(*days):
    return {d: (f"f-{d}", f"s-{d}") for d in days}


def test_days_are_compared_once_per_pair_of_files():
    d1, d2, d3 = date(2026, 10, 9), date(2026, 10, 10), date(2026, 10, 11)
    left, right = _files(d1, d2, d3), _files(d1, d2)
    compared = {(d1, "s-" + str(d1), "s-" + str(d1))}
    days = days_to_compare(left, right, compared, DAY, grace_days=2)
    assert [d.day for d in days] == [d2]
    right[d1] = ("f-new", "s-new")
    assert [d.day for d in days_to_compare(left, right, compared, DAY, 2)] == [d1, d2]


def test_a_day_on_one_venue_waits_for_the_other_then_counts():
    d = date(2026, 10, 9)
    assert days_to_compare(_files(d), {}, set(), date(2026, 10, 11), 2) == []
    (late,) = days_to_compare(_files(d), {}, set(), date(2026, 10, 12), 2)
    assert late == DayFiles(d, f"f-{d}", f"s-{d}", None, None)


def test_a_period_recompares_its_days_even_if_done():
    d1, d2 = date(2026, 9, 1), date(2026, 9, 2)
    files = _files(d1, d2)
    compared = {(d, f"s-{d}", f"s-{d}") for d in (d1, d2)}
    days = days_to_compare(files, files, compared, DAY, 2, period=(d2, d2))
    assert [d.day for d in days] == [d2]


def test_parse_args():
    args = parse_args(["--catalog", "pitlake_dev", "--inject-fault", "knowable_at_missing"])
    assert args.fault == "none" and args.period is None
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--inject-fault", "venue_price_divergence"])
    args = parse_args(
        [
            "--catalog",
            "pitlake_dev",
            "--inject-fault",
            "venue_missing_minutes",
            "--period-start",
            "2025-01-02",
            "--period-end",
            "2025-01-03",
        ]
    )
    assert args.fault == "venue_missing_minutes"
    assert args.period == (date(2025, 1, 2), date(2025, 1, 3))


def test_queries_and_rows():
    query = uncommitted_log_query("pitlake_prod")
    assert "WHERE processed_at > (SELECT coalesce(max(committed_at)" in query
    assert "`pitlake_prod`.`control`.`silver_commits`" in query
    assert day_predicate(BTC, [date(2025, 1, 2), date(2025, 1, 3)]) == (
        "pair = 'BTC' AND day IN (DATE'2025-01-02', DATE'2025-01-03')"
    )
    (result,) = judge([_rate("x.y", 100, 50)], THRESHOLDS, {})
    assert result_rows([result], "9", NOW, "none")[0][2:9] == (
        REJECTION_RATE,
        "x.y",
        DAY,
        100,
        50,
        3.0,
        False,
    )
    files = DayFiles(DAY, "l", "ls", "r", "rs")
    assert log_rows(BTC, [files], {DAY: (1440, 1, 2, 3)}, "9", NOW) == [
        ("BTC", DAY, "l", "ls", "r", "rs", 1440, 1, 2, 3, NOW, "9")
    ]
    assert threshold_rows(THRESHOLDS, NOW)[0][:4] == (REJECTION_RATE, ANY, 0.01, 2)


def test_comparison_table_columns_follow_the_comparison():
    ddl = cross_venue_ddl("pitlake_dev")
    names = [f"{side}_{c}" for side, c, _ in COMPARISON_VALUES]
    positions = [ddl.index(f"{n} ") for n in names]
    assert positions == sorted(positions)
    assert "CLUSTER BY (pair, event_time)" in ddl
    assert "gate_check STRING, subject STRING, day DATE, reason STRING" in quality_waivers_ddl(
        "pitlake_dev"
    )

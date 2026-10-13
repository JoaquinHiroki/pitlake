import math
from datetime import UTC, date, datetime

import pytest

from pitlake.backtest import (
    COST_BPS,
    GRID,
    MAX_LEVERAGE,
    Asset,
    Day,
    Params,
    decision_times,
    differences,
    input_fingerprint,
    observations,
    run,
    summarize,
    target_weight,
)

X = Asset("X", 365, False)
STOCK = Asset("S", 252, True)
PARAMS = Params(lookback=1, target_vol=0.2, macro_brake=False, require_profit=False)
TIMES = decision_times(date(2025, 1, 1), date(2025, 1, 4))


def bar(day):
    return datetime(2025, 1, day, tzinfo=UTC)


def world(closes, vol=0.01, entity="X", extra=None):
    rows = []
    for at, (day, close) in zip(TIMES, closes, strict=True):
        rows.append((entity, at, "close", close, bar(day)))
        rows.append((entity, at, "volatility_20d", vol, bar(day)))
        for feature, value in (extra or {}).items():
            rows.append((entity, at, feature, value, bar(day)))
    return observations(rows)


def test_decisions_are_daily_at_six_utc():
    assert TIMES == [datetime(2025, 1, d, 6, tzinfo=UTC) for d in range(1, 5)]
    with pytest.raises(ValueError, match="before it starts"):
        decision_times(date(2025, 1, 2), date(2025, 1, 1))


def test_params_round_trip_and_the_grid():
    assert Params.from_json(PARAMS.to_json()) == PARAMS
    assert PARAMS.to_json() == (
        '{"cost_bps": 5.0, "lookback": 1, "macro_brake": false, "require_profit": false, '
        '"target_vol": 0.2}'
    )
    assert PARAMS.label == "lb1-vol0.2-nobrake-any"
    assert len(GRID) == 24 and len({p.label for p in GRID}) == 24
    with pytest.raises(ValueError, match="lookback"):
        Params(0, 0.2, False, False)
    with pytest.raises(ValueError, match="target_vol"):
        Params(20, 1.5, False, False)


def test_observations_read_naive_times_as_utc_and_skip_missing_values():
    seen = observations(
        [
            ("X", datetime(2025, 1, 1, 6), "close", 100, datetime(2025, 1, 1)),
            ("X", datetime(2025, 1, 1, 6), "net_income_q", None, datetime(2025, 1, 1)),
        ]
    )
    assert seen == {("X", TIMES[0]): {"close": (100.0, bar(1))}}


def test_the_input_fingerprint_ignores_order_but_not_values():
    rows = [("X", TIMES[0], "close", 100.0, bar(1)), ("Y", TIMES[0], "close", 5.0, bar(1))]
    same = input_fingerprint(observations(rows)) == input_fingerprint(observations(rows[::-1]))
    assert same
    changed = [rows[0], ("Y", TIMES[0], "close", 5.000001, bar(1))]
    assert input_fingerprint(observations(rows)) != input_fingerprint(observations(changed))


def test_size_targets_volatility_after_a_rise_and_is_capped():
    closes = [(bar(1), 100.0), (bar(2), 101.0)]
    weight = target_weight(X, {"volatility_20d": (0.01, bar(2))}, closes, PARAMS, 1)
    assert weight == pytest.approx(0.2 / (0.01 * math.sqrt(365)))
    calm = target_weight(X, {"volatility_20d": (0.0001, bar(2))}, closes, PARAMS, 2)
    assert calm == MAX_LEVERAGE / 2
    falling = [(bar(1), 101.0), (bar(2), 100.0)]
    assert target_weight(X, {"volatility_20d": (0.01, bar(2))}, falling, PARAMS, 1) == 0
    assert target_weight(X, {"volatility_20d": (0.01, bar(2))}, closes[:1], PARAMS, 1) == 0
    assert target_weight(X, {}, closes, PARAMS, 1) == 0


def test_filters_use_only_what_was_known():
    closes = [(bar(1), 100.0), (bar(2), 101.0)]
    vol = {"volatility_20d": (0.01, bar(2))}
    strict = Params(1, 0.2, False, True)
    assert target_weight(STOCK, vol, closes, strict, 1) == 0
    assert target_weight(STOCK, {**vol, "net_income_q": (-1.0, bar(1))}, closes, strict, 1) == 0
    profitable = target_weight(STOCK, {**vol, "net_income_q": (5.0, bar(1))}, closes, strict, 1)
    assert profitable > 0
    assert target_weight(X, vol, closes, strict, 1) > 0
    braked = Params(1, 0.2, True, False)
    hot = target_weight(X, {**vol, "cpi_yoy": (0.031, bar(1))}, closes, braked, 1)
    cool = target_weight(X, {**vol, "cpi_yoy": (0.029, bar(1))}, closes, braked, 1)
    assert hot == pytest.approx(cool / 2)


def test_positions_earn_the_next_return_and_pay_for_turnover():
    seen = world([(1, 100.0), (2, 101.0), (3, 103.0), (3, 103.0)])
    result = run(seen, TIMES, PARAMS, (X,))
    size = 0.2 / (0.01 * math.sqrt(365))
    assert [d.pnl for d in result.days] == pytest.approx([0, 0, size * (103 / 101 - 1), 0])
    assert [d.turnover for d in result.days] == pytest.approx([0, size, 0, 0])
    assert result.days[1].cost == pytest.approx(size * COST_BPS / 10000)
    assert result.days[3].gross == pytest.approx(size)
    assert result.metrics["days"] == 4


def test_a_repeated_bar_does_not_count_as_a_new_observation():
    seen = world([(1, 100.0), (1, 100.0), (2, 101.0), (3, 102.0)])
    result = run(seen, TIMES, PARAMS, (X,))
    assert [d.gross > 0 for d in result.days] == [False, False, True, True]


def test_the_same_inputs_give_the_same_fingerprint_and_others_do_not():
    closes = [(1, 100.0), (2, 101.0), (3, 103.0), (4, 102.0)]
    first, again = run(world(closes), TIMES, PARAMS, (X,)), run(world(closes), TIMES, PARAMS, (X,))
    assert first == again
    moved = run(world([*closes[:3], (4, 102.000001)]), TIMES, PARAMS, (X,))
    assert moved.fingerprint != first.fingerprint
    assert (
        differences(
            {"input_hash": "a", "result_hash": first.fingerprint, "metrics": first.metrics},
            "a",
            first,
        )
        == []
    )
    found = differences(
        {"input_hash": "a", "result_hash": first.fingerprint, "metrics": first.metrics}, "b", moved
    )
    assert found[0] == "input a -> b"
    assert any(f.startswith("result ") for f in found)
    assert any(f.startswith("total_return ") for f in found)


def test_summary_metrics():
    at = TIMES[0]
    days = [Day(at, 0.1, 0.0, 1.0, 1.0), Day(at, -0.1, 0.0, 1.0, 0.0)]
    metrics = summarize(days)
    assert metrics["total_return"] == pytest.approx(1.1 * 0.9 - 1)
    assert metrics["max_drawdown"] == pytest.approx(0.1)
    assert metrics["annual_volatility"] == pytest.approx(math.sqrt(0.02) * math.sqrt(365))
    assert metrics["sharpe"] == 0
    assert metrics["turnover"] == 1.0 and metrics["days"] == 2.0
    assert summarize([])["days"] == 0.0
    assert summarize([Day(at, 0.0, 0.0, 0.0, 0.0)])["annual_volatility"] == 0.0

import json
import logging
from datetime import date
from pathlib import Path

import pytest

from pitlake_collector.config import load_config, parse_config
from pitlake_collector.logs import JsonFormatter

CONFIGS = Path(__file__).parents[1]


def test_example_config_is_valid():
    config = load_config(CONFIGS / "config.example.toml")
    assert config.catalog == "pitlake_dev"
    assert config.feeds[0].symbols == ("BTCUSDT",)
    assert config.feeds[0].start_date == date(2025, 1, 1)
    assert config.feeds[0].end_date is None


def test_shipped_configs_target_their_catalogs():
    dev = load_config(CONFIGS / "config.dev.toml")
    prod = load_config(CONFIGS / "config.prod.toml")
    assert (dev.catalog, prod.catalog) == ("pitlake_dev", "pitlake_prod")
    # Prod keeps up with new days; the daily ingest schedule collects up to yesterday.
    # Both venues cover the same days, so Stage 4 can compare them.
    assert {(f.source, f.start_date) for f in prod.feeds} == {
        ("binance", date(2025, 1, 1)),
        ("coinbase", date(2025, 1, 1)),
        # Macro vintages start earlier, so the one in force on the first trading day exists.
        ("fred", date(2024, 1, 1)),
        ("alpaca", date(2025, 1, 1)),
        ("edgar", date(2024, 1, 1)),
    }
    alpaca = [f for f in prod.feeds if f.source == "alpaca"]
    assert {f.dataset for f in alpaca} == {"stock_trades", "stock_bars_1d"}
    assert all(f.symbols == ("SPY", "AAPL", "MSFT", "NVDA") for f in alpaca)
    # The same companies' filings, so Stage 5 can join them; SPY files no financial statements.
    edgar = [f for f in prod.feeds if f.source == "edgar"]
    assert {f.dataset for f in edgar} == {"company_facts", "filings"}
    assert all(f.symbols == ("AAPL", "MSFT", "NVDA") for f in edgar)
    assert all(f.end_date is None for f in prod.feeds)


def _data(**feed):
    base = {"source": "binance", "dataset": "spot_trades", "symbols": ["BTCUSDT"]}
    base.update(feed)
    return {"catalog": "pitlake_dev", "feeds": [base]}


def test_string_dates_are_accepted():
    config = parse_config(_data(start_date="2025-01-01", end_date="2025-12-31"))
    assert config.feeds[0].end_date == date(2025, 12, 31)


@pytest.mark.parametrize(
    "data",
    [
        {"catalog": "pitlake_dev", "feeds": []},
        {
            "catalog": "Bad-Name",
            **{k: v for k, v in _data(start_date="2025-01-01").items() if k != "catalog"},
        },
        _data(start_date="2025-02-01", end_date="2025-01-01"),
        _data(start_date="2025-01-01", symbols=[]),
    ],
)
def test_invalid_configs_are_rejected(data):
    with pytest.raises(ValueError):
        parse_config(data)


def test_json_log_lines_carry_extra_fields():
    record = logging.LogRecord("c", logging.INFO, __file__, 1, "landed", None, None)
    record.file = "f.zip"
    line = json.loads(JsonFormatter().format(record))
    assert line["event"] == "landed" and line["file"] == "f.zip" and line["level"] == "INFO"

import base64
import io
import json
import logging
from dataclasses import dataclass
from datetime import date

import pytest

from fakes import FakeHttp
from pitlake_collector.config import CollectorConfig, Feed
from pitlake_collector.http import redact
from pitlake_collector.landing import LocalLanding
from pitlake_collector.logs import JsonFormatter
from pitlake_collector.secrets import env_name, secret_reader
from pitlake_collector.sources.fred import BASE_URL
from pitlake_collector.sync import run_cycle


@dataclass
class _Secret:
    value: str


class FakeSecretsApi:
    def __init__(self, values: dict[tuple[str, str], str]):
        self.values = values
        self.calls = 0

    def get_secret(self, scope: str, key: str) -> _Secret:
        self.calls += 1
        return _Secret(base64.b64encode(self.values[(scope, key)].encode()).decode())


class FakeClient:
    def __init__(self, values):
        self.secrets = FakeSecretsApi(values)


def test_env_name():
    assert env_name("fred-api-key") == "PITLAKE_SECRET_FRED_API_KEY"


def test_reads_the_scope_once_per_secret(monkeypatch):
    monkeypatch.delenv("PITLAKE_SECRET_FRED_API_KEY", raising=False)
    client = FakeClient({("pitlake", "fred-api-key"): "abc\n"})
    read = secret_reader("pitlake", client)
    assert read("fred-api-key") == "abc"
    assert read("fred-api-key") == "abc"
    assert client.secrets.calls == 1


def test_environment_wins_over_the_scope(monkeypatch):
    monkeypatch.setenv("PITLAKE_SECRET_FRED_API_KEY", "from-env")
    client = FakeClient({})
    assert secret_reader("pitlake", client)("fred-api-key") == "from-env"
    assert client.secrets.calls == 0


def test_rejects_odd_secret_names():
    with pytest.raises(ValueError):
        secret_reader("pitlake", FakeClient({}))("../etc")


@pytest.mark.parametrize(
    "text",
    [
        "400 Client Error: Bad Request for url: https://x/obs?series_id=A&api_key=SECRET123&x=1",
        "Max retries exceeded with url: /fred/series?api_key=SECRET123 (Caused by ...)",
        "GET /path?token=SECRET123",
    ],
)
def test_redact(text):
    assert "SECRET123" not in redact(text)
    assert "REDACTED" in redact(text)


def test_log_lines_are_redacted():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("test_redaction")
    logger.addHandler(handler)
    try:
        logger.warning("Retrying after error: /fred?api_key=SECRET123", extra={"url": "?apikey=X1"})
    finally:
        logger.removeHandler(handler)
    line = stream.getvalue()
    assert "SECRET123" not in line and "X1" not in line
    json.loads(line)


def test_a_feed_without_its_secret_fails_alone(tmp_path):
    feeds = (
        Feed("fred", "series_vintages", ("UNRATE",), date(2025, 1, 1)),
        Feed("binance", "spot_trades", ("BTCUSDT",), date(2025, 1, 1), date(2025, 1, 1)),
    )
    config = CollectorConfig(catalog="pitlake_dev", work_dir=tmp_path / "work", feeds=feeds)

    def no_key(name: str) -> str:
        raise KeyError(name)

    http = FakeHttp(
        {
            "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
            "BTCUSDT-trades-2025-01-01.zip": b"z"
        }
    )
    results = run_cycle(
        config, LocalLanding(tmp_path / "l"), http, today=date(2025, 1, 4), secrets=no_key
    )
    assert [(r.feed, r.failed, r.landed) for r in results] == [
        ("fred.series_vintages", ["setup"], 0),
        ("binance.spot_trades", [], 1),
    ]


def test_a_failed_listing_fails_only_its_feed(tmp_path):
    feeds = (Feed("fred", "series_vintages", ("UNRATE",), date(2025, 1, 1)),)
    config = CollectorConfig(catalog="pitlake_dev", work_dir=tmp_path / "work", feeds=feeds)
    http = FakeHttp({})  # every FRED request is a 404
    [result] = run_cycle(
        config,
        LocalLanding(tmp_path / "l"),
        http,
        today=date(2025, 1, 4),
        secrets=lambda name: "0123456789abcdef0123456789abcdef",
    )
    assert result.failed == ["listing"]
    assert http.requests[0].startswith(f"{BASE_URL}/series/vintagedates?series_id=UNRATE")

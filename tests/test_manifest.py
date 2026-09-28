import json
from dataclasses import fields
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from pitlake.manifest import SPARK_SCHEMA, LandingManifest


def _manifest(**overrides) -> LandingManifest:
    values = dict(
        source="binance",
        dataset="spot_trades",
        symbol="BTCUSDT",
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 1),
        file_name="BTCUSDT-trades-2025-01-01.zip",
        landing_path="binance/spot_trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip",
        source_url="https://data.binance.vision/x.zip",
        sha256="a" * 64,
        size_bytes=123,
        fetched_at=datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        collector_version="0.1.0",
        collector_host="vm",
    )
    values.update(overrides)
    return LandingManifest(**values)


def test_spark_schema_lists_exactly_the_dataclass_fields():
    schema_fields = [part.strip().split()[0] for part in SPARK_SCHEMA.split(",")]
    assert sorted(schema_fields) == sorted(f.name for f in fields(LandingManifest))


def test_json_is_one_line_with_spark_friendly_formats():
    text = _manifest().to_json()
    assert "\n" not in text
    record = json.loads(text)
    assert record["period_start"] == "2025-01-01"
    assert record["fetched_at"] == "2025-01-02T03:04:05.678Z"
    assert record["schema_version"] == 1


def test_round_trip_keeps_values_to_the_millisecond():
    original = _manifest()
    parsed = LandingManifest.from_json(original.to_json())
    assert parsed.fetched_at == original.fetched_at.replace(microsecond=678000)
    assert parsed.sha256 == original.sha256 and parsed.period_end == original.period_end


def test_fetched_at_is_normalised_to_utc():
    local = datetime(2025, 1, 2, 11, 0, tzinfo=timezone(timedelta(hours=8)))
    assert json.loads(_manifest(fetched_at=local).to_json())["fetched_at"] == (
        "2025-01-02T03:00:00.000Z"
    )


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError):
        _manifest(fetched_at=datetime(2025, 1, 2)).to_json()


def test_unknown_fields_from_newer_collectors_are_ignored():
    record = json.loads(_manifest().to_json())
    record["added_later"] = "x"
    assert LandingManifest.from_json(json.dumps(record)).symbol == "BTCUSDT"

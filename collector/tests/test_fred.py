import json
from datetime import date

import pytest

from fakes import FakeHttp
from pitlake_collector.sources.base import InvalidResponse
from pitlake_collector.sources.fred import (
    BASE_URL,
    FredVintages,
    parse_vintage_dates,
    validate_vintage,
)

KEY = "0123456789abcdef0123456789abcdef"
START, END = date(2025, 1, 1), date(2025, 1, 31)
DATES_URL = (
    f"{BASE_URL}/series/vintagedates?series_id=UNRATE&realtime_start=2025-01-01"
    f"&realtime_end=2025-01-31&file_type=json&limit=10000&api_key={KEY}"
)


def _obs_url(day: str) -> str:
    return (
        f"{BASE_URL}/series/observations?series_id=UNRATE&realtime_start={day}"
        f"&realtime_end={day}&file_type=json&limit=100000"
    )


def _compact(data) -> bytes:
    return json.dumps(data, separators=(",", ":")).encode()


def _dates(*days: str) -> bytes:
    return _compact({"count": len(days), "offset": 0, "limit": 10000, "vintage_dates": list(days)})


def _vintage(day: str, values=(("2024-11-01", "4.2"), ("2024-12-01", "4.1"))) -> bytes:
    observations = [
        {"realtime_start": day, "realtime_end": day, "date": d, "value": v} for d, v in values
    ]
    return _compact(
        {"realtime_start": day, "count": len(observations), "observations": observations}
    )


def _source(files: dict[str, bytes]) -> FredVintages:
    return FredVintages(FakeHttp(files), api_key=KEY)


def test_one_partition_per_vintage_date_and_no_key_in_partition_urls():
    source = _source({DATES_URL: _dates("2025-01-10", "2025-01-31")})
    parts = source.partitions("UNRATE", START, END)
    assert [p.file_name for p in parts] == [
        "UNRATE-vintage-2025-01-10.jsonl",
        "UNRATE-vintage-2025-01-31.jsonl",
    ]
    assert parts[0].relpath == "fred/series_vintages/UNRATE/UNRATE-vintage-2025-01-10.jsonl"
    assert parts[0].url == _obs_url("2025-01-10")
    assert all(KEY not in p.url for p in parts)


def test_fetch_lands_the_response_and_sends_the_key(tmp_path):
    body = _vintage("2025-01-10")
    http = FakeHttp(
        {DATES_URL: _dates("2025-01-10"), _obs_url("2025-01-10") + f"&api_key={KEY}": body}
    )
    source = FredVintages(http, api_key=KEY)
    [partition] = source.partitions("UNRATE", START, END)
    fetched = source.fetch(partition, tmp_path)
    assert fetched.local_path.read_bytes() == body + b"\n"
    assert all(url.endswith(f"&api_key={KEY}") for url in http.requests)


def test_vintage_dates_outside_the_window_are_ignored():
    assert parse_vintage_dates(_dates("2024-12-06", "2025-01-10"), START, END) == [
        date(2025, 1, 10)
    ]


@pytest.mark.parametrize(
    "body",
    [
        b"<html>",
        b'{"error_code":400,"error_message":"Bad Request."}',
        b'{"count":3,"vintage_dates":["2025-01-10"]}',
        b'{"vintage_dates":["10/01/2025"]}',
    ],
)
def test_vintage_dates_rejects(body):
    with pytest.raises(InvalidResponse):
        parse_vintage_dates(body, START, END)


VINTAGE = date(2025, 1, 10)


@pytest.mark.parametrize(
    "body",
    [
        b'{"observations":[]}',
        b'{"count":5,"observations":[{"realtime_start":"2025-01-10","realtime_end":"2025-01-10",'
        b'"date":"2024-12-01","value":"4.1"}]}',
        _vintage("2025-01-09"),
        _vintage("2025-01-10", (("2024-12-01", "4.1"), ("2024-11-01", "4.2"))),
        _vintage("2025-01-10", (("2024-12-01", "4.1"), ("2024-12-01", "4.0"))),
        _vintage("2025-01-10").replace(b'"4.1"', b"4.1"),
        _vintage("2025-01-10").replace(b"},{", b"},\n{"),
    ],
)
def test_validate_vintage_rejects(body):
    with pytest.raises(InvalidResponse):
        validate_vintage(body, VINTAGE)


def test_missing_values_are_accepted_as_published():
    assert validate_vintage(_vintage("2025-01-10", (("2024-12-01", "."),)), VINTAGE) == 1


@pytest.mark.parametrize("bad", ["", "short", KEY.upper(), KEY + "&x=1"])
def test_malformed_api_key_is_refused_without_echoing_it(bad):
    with pytest.raises(ValueError) as info:
        FredVintages(FakeHttp({}), api_key=bad)
    assert not bad or bad not in str(info.value)


def test_create_reads_the_key_from_secrets():
    asked = []
    source = FredVintages.create(FakeHttp({}), lambda name: asked.append(name) or KEY)
    assert asked == ["fred-api-key"] and isinstance(source, FredVintages)

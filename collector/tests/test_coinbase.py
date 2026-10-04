import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise

import pytest

from fakes import FakeHttp
from pitlake_collector.sources.base import InvalidResponse, NotPublished
from pitlake_collector.sources.coinbase import (
    BASE_URL,
    CoinbaseCandles,
    day_windows,
    validate_candles,
)

DAY = date(2025, 1, 1)
PRODUCT_URL = f"{BASE_URL}/products/BTC-USD/candles?granularity=60"


def _url(start: datetime, end: datetime) -> str:
    return f"{PRODUCT_URL}&start={start:%Y-%m-%dT%H:%M:%SZ}&end={end:%Y-%m-%dT%H:%M:%SZ}"


def _body(start: datetime, end: datetime) -> bytes:
    """A full window as Coinbase serves it: compact JSON, newest candle first."""
    first, last = int(start.timestamp()), int(end.timestamp())
    candles = [[t, 1.5, 2.5, 2, 2.25, 0.001] for t in range(last, first - 1, -60)]
    return json.dumps(candles, separators=(",", ":")).encode()


def _full_day(day: date = DAY) -> dict[str, bytes]:
    return {_url(s, e): _body(s, e) for s, e in day_windows(day)}


def test_day_windows_cover_the_day_in_300_minute_steps():
    windows = day_windows(DAY)
    assert len(windows) == 5
    assert windows[0] == (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, 4, 59, tzinfo=UTC))
    assert windows[-1] == (
        datetime(2025, 1, 1, 20, tzinfo=UTC),
        datetime(2025, 1, 1, 23, 59, tzinfo=UTC),
    )
    minutes = sum((e - s).total_seconds() / 60 + 1 for s, e in windows)
    assert minutes == 1440
    # Each window starts one minute after the previous one ends: no gap, no overlap.
    assert all(b[0] - a[1] == timedelta(minutes=1) for a, b in pairwise(windows))


def test_partitions_one_jsonl_file_per_day():
    parts = CoinbaseCandles(FakeHttp({})).partitions("BTC-USD", DAY, date(2025, 1, 2))
    assert [p.file_name for p in parts] == [
        "BTC-USD-candles-1m-2025-01-01.jsonl",
        "BTC-USD-candles-1m-2025-01-02.jsonl",
    ]
    assert parts[0].relpath == (
        "coinbase/spot_candles_1m/BTC-USD/BTC-USD-candles-1m-2025-01-01.jsonl"
    )
    assert parts[0].url.endswith("start=2025-01-01T00:00:00Z&end=2025-01-01T23:59:00Z")


@pytest.mark.parametrize("bad", ["BTCUSD", "btc-usd", "BTC/USD", "../x", "BTC-USD-X"])
def test_product_validation(bad):
    with pytest.raises(ValueError):
        CoinbaseCandles(FakeHttp({})).validate_symbol(bad)


def test_fetch_lands_every_response_byte_for_byte(tmp_path):
    served = _full_day()
    source = CoinbaseCandles(FakeHttp(served))
    [partition] = source.partitions("BTC-USD", DAY, DAY)
    fetched = source.fetch(partition, tmp_path)

    lines = fetched.local_path.read_bytes().splitlines()
    assert lines == [served[_url(s, e)] for s, e in day_windows(DAY)]
    assert fetched.sha256 == hashlib.sha256(fetched.local_path.read_bytes()).hexdigest()
    assert sum(len(json.loads(line)) for line in lines) == 1440


def test_minutes_without_trades_are_allowed(tmp_path):
    served = _full_day()
    first = _url(*day_windows(DAY)[0])
    served[first] = json.dumps(json.loads(served[first])[5:]).encode().replace(b" ", b"")
    source = CoinbaseCandles(FakeHttp(served))
    [partition] = source.partitions("BTC-USD", DAY, DAY)
    assert source.fetch(partition, tmp_path).size_bytes > 0


def test_a_day_with_no_candles_is_not_published(tmp_path):
    source = CoinbaseCandles(FakeHttp({url: b"[]" for url in _full_day()}))
    [partition] = source.partitions("BTC-USD", DAY, DAY)
    with pytest.raises(NotPublished):
        source.fetch(partition, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_an_invalid_window_lands_nothing(tmp_path):
    served = _full_day()
    served[_url(*day_windows(DAY)[2])] = b'{"message":"rate limited"}'
    source = CoinbaseCandles(FakeHttp(served))
    [partition] = source.partitions("BTC-USD", DAY, DAY)
    with pytest.raises(InvalidResponse):
        source.fetch(partition, tmp_path)
    assert list(tmp_path.iterdir()) == []


START, END = day_windows(DAY)[0]
T0 = int(START.timestamp())


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b'{"message":"x"}',
        b"[[1,2,3]]",
        f'[[{T0},1,2,3,4,"5"]]'.encode(),
        f"[[{T0 - 60},1,2,3,4,5]]".encode(),
        f"[[{T0 + 300 * 60},1,2,3,4,5]]".encode(),
        f"[[{T0 + 1},1,2,3,4,5]]".encode(),
        f"[[{T0},true,2,3,4,5]]".encode(),
        f"[[{T0},1,2,3,4,5],\n[{T0 + 60},1,2,3,4,5]]".encode(),
    ],
)
def test_validate_candles_rejects(body):
    with pytest.raises(InvalidResponse):
        validate_candles(body, START, END)


def test_duplicate_candles_are_rejected(tmp_path):
    served = _full_day()
    windows = day_windows(DAY)
    # The second window serves its first candle twice.
    second = json.loads(served[_url(*windows[1])])
    second.append([int(windows[1][0].timestamp()), 1, 2, 3, 4, 5])
    served[_url(*windows[1])] = json.dumps(second, separators=(",", ":")).encode()
    source = CoinbaseCandles(FakeHttp(served))
    [partition] = source.partitions("BTC-USD", DAY, DAY)
    with pytest.raises(InvalidResponse):
        source.fetch(partition, tmp_path)

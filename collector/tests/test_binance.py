from datetime import date

import pytest

from fakes import FakeHttp
from pitlake_collector.sources.base import ChecksumMismatch, NotPublished
from pitlake_collector.sources.binance import BASE_URL, BinanceSpotTrades, parse_checksum

URL = f"{BASE_URL}/BTCUSDT/BTCUSDT-trades-2025-01-01.zip"


def test_partitions_cover_each_day_inclusive():
    parts = BinanceSpotTrades(FakeHttp({})).partitions(
        "BTCUSDT", date(2024, 12, 31), date(2025, 1, 2)
    )
    assert [p.file_name for p in parts] == [
        "BTCUSDT-trades-2024-12-31.zip",
        "BTCUSDT-trades-2025-01-01.zip",
        "BTCUSDT-trades-2025-01-02.zip",
    ]
    assert parts[1].url == URL
    assert parts[1].relpath == "binance/spot_trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip"
    assert parts[1].period_start == parts[1].period_end == date(2025, 1, 1)


@pytest.mark.parametrize("bad", ["btcusdt", "BTC/USDT", "BTC USDT", "../x", "B"])
def test_symbol_validation(bad):
    with pytest.raises(ValueError):
        BinanceSpotTrades(FakeHttp({})).validate_symbol(bad)


def test_latest_available_is_yesterday():
    assert BinanceSpotTrades(FakeHttp({})).latest_available(date(2025, 3, 1)) == date(2025, 2, 28)


def test_parse_checksum():
    digest = "ab" * 32
    assert parse_checksum(f"{digest}  f.zip\n", "f.zip") == digest
    with pytest.raises(ChecksumMismatch):
        parse_checksum(f"{digest}  other.zip\n", "f.zip")
    with pytest.raises(ChecksumMismatch):
        parse_checksum("<html>error</html>", "f.zip")


def test_fetch_verifies_checksum(tmp_path):
    source = BinanceSpotTrades(FakeHttp({URL: b"zipbytes"}))
    [partition] = source.partitions("BTCUSDT", date(2025, 1, 1), date(2025, 1, 1))
    fetched = source.fetch(partition, tmp_path)
    assert fetched.local_path.read_bytes() == b"zipbytes"
    assert fetched.size_bytes == 8
    assert fetched.fetched_at.tzinfo is not None


def test_fetch_rejects_and_deletes_corrupt_download(tmp_path):
    source = BinanceSpotTrades(FakeHttp({URL: b"zipbytes"}, checksums={URL: "0" * 64}))
    [partition] = source.partitions("BTCUSDT", date(2025, 1, 1), date(2025, 1, 1))
    with pytest.raises(ChecksumMismatch):
        source.fetch(partition, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_missing_file_is_not_published(tmp_path):
    source = BinanceSpotTrades(FakeHttp({}))
    [partition] = source.partitions("BTCUSDT", date(2025, 1, 1), date(2025, 1, 1))
    with pytest.raises(NotPublished):
        source.fetch(partition, tmp_path)

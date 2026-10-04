"""Binance spot trades from the public data archive (data.binance.vision).

One zip per trading pair per UTC day, each with a .CHECKSUM file holding its SHA-256.
Files for day D are published during day D+1.
"""

import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pitlake_collector.http import NotFound
from pitlake_collector.sources.base import (
    ChecksumMismatch,
    FetchedFile,
    Http,
    NotPublished,
    Partition,
    Source,
)

BASE_URL = "https://data.binance.vision/data/spot/daily/trades"

_SYMBOL = re.compile(r"^[A-Z0-9]{2,20}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def parse_checksum(text: str, file_name: str) -> str:
    """Parse a sha256sum-style line: '<hex>  <file name>'."""
    parts = text.split()
    if len(parts) != 2 or parts[1] != file_name or not _SHA256.fullmatch(parts[0].lower()):
        raise ChecksumMismatch(f"unexpected CHECKSUM content for {file_name}: {text[:200]!r}")
    return parts[0].lower()


class BinanceSpotTrades(Source):
    name = "binance"
    dataset = "spot_trades"

    def __init__(self, http: Http, base_url: str = BASE_URL) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    def validate_symbol(self, symbol: str) -> str:
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError(f"invalid Binance symbol: {symbol!r}")
        return symbol

    def latest_available(self, today: date) -> date:
        return today - timedelta(days=1)

    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        self.validate_symbol(symbol)
        days = (end - start).days + 1
        result = []
        for offset in range(max(days, 0)):
            day = start + timedelta(days=offset)
            file_name = f"{symbol}-trades-{day.isoformat()}.zip"
            result.append(
                Partition(
                    source=self.name,
                    dataset=self.dataset,
                    symbol=symbol,
                    period_start=day,
                    period_end=day,
                    file_name=file_name,
                    url=f"{self._base_url}/{symbol}/{file_name}",
                )
            )
        return result

    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        try:
            expected = parse_checksum(
                self._http.get_text(partition.url + ".CHECKSUM"), partition.file_name
            )
            dest = dest_dir / partition.file_name
            sha256, size = self._http.download(partition.url, dest)
        except NotFound as exc:
            raise NotPublished(partition.url) from exc
        if sha256 != expected:
            dest.unlink(missing_ok=True)
            raise ChecksumMismatch(f"{partition.file_name}: got {sha256}, expected {expected}")
        return FetchedFile(
            partition=partition,
            local_path=dest,
            sha256=sha256,
            size_bytes=size,
            fetched_at=datetime.now(UTC),
        )

"""Coinbase Exchange one-minute candles from the public REST API (api.exchange.coinbase.com).

One landed file per product per UTC day, holding the day's candles as served (ADR 0004). The
endpoint returns at most 300 candles per request, so a day is fetched in five windows; both ends of
a window are inclusive. Each response is a JSON array of [time, low, high, open, close, volume],
newest first. Minutes with no trades have no candle.
"""

import hashlib
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from pitlake_collector.sources.base import (
    FetchedFile,
    Http,
    InvalidResponse,
    NotPublished,
    Partition,
    Source,
)

BASE_URL = "https://api.exchange.coinbase.com"
GRANULARITY_SECONDS = 60
CANDLES_PER_REQUEST = 300

_PRODUCT = re.compile(r"^[A-Z0-9]{2,10}-[A-Z0-9]{2,10}$")
_CANDLE_FIELDS = 6
_STEP = timedelta(seconds=GRANULARITY_SECONDS)
_WINDOW = _STEP * CANDLES_PER_REQUEST


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def day_windows(day: date) -> list[tuple[datetime, datetime]]:
    """Inclusive [start, end] windows of at most 300 candles covering one UTC day."""
    start = datetime.combine(day, time(), tzinfo=UTC)
    last = start + timedelta(days=1) - _STEP
    windows = []
    while start <= last:
        windows.append((start, min(start + _WINDOW - _STEP, last)))
        start += _WINDOW
    return windows


def validate_candles(body: bytes, start: datetime, end: datetime) -> list[int]:
    """Check one response and return its candle times. Raises InvalidResponse."""
    if b"\n" in body or b"\r" in body:
        raise InvalidResponse("response body contains a line break; it cannot be one JSON line")
    try:
        candles = json.loads(body)
    except ValueError as exc:
        raise InvalidResponse(f"response is not JSON: {body[:200]!r}") from exc
    if not isinstance(candles, list):
        raise InvalidResponse(f"expected a JSON array of candles, got: {body[:200]!r}")
    lo, hi = int(start.timestamp()), int(end.timestamp())
    times = []
    for candle in candles:
        if (
            not isinstance(candle, list)
            or len(candle) != _CANDLE_FIELDS
            or not all(isinstance(v, int | float) and not isinstance(v, bool) for v in candle)
        ):
            raise InvalidResponse(f"unexpected candle: {candle!r}")
        ts = candle[0]
        if not isinstance(ts, int) or not lo <= ts <= hi or ts % GRANULARITY_SECONDS:
            raise InvalidResponse(f"candle time {ts!r} outside window {_iso(start)}..{_iso(end)}")
        times.append(ts)
    if len(set(times)) != len(times):
        raise InvalidResponse(f"duplicate candle times in window {_iso(start)}..{_iso(end)}")
    return times


class CoinbaseCandles(Source):
    name = "coinbase"
    dataset = "spot_candles_1m"

    def __init__(self, http: Http, base_url: str = BASE_URL) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    def validate_symbol(self, symbol: str) -> str:
        if not _PRODUCT.fullmatch(symbol):
            raise ValueError(f"invalid Coinbase product id: {symbol!r}")
        return symbol

    def latest_available(self, today: date) -> date:
        # A day's candles are complete once the day is over.
        return today - timedelta(days=1)

    def _url(self, symbol: str, start: datetime, end: datetime) -> str:
        return (
            f"{self._base_url}/products/{symbol}/candles"
            f"?granularity={GRANULARITY_SECONDS}&start={_iso(start)}&end={_iso(end)}"
        )

    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        self.validate_symbol(symbol)
        result = []
        for offset in range(max((end - start).days + 1, 0)):
            day = start + timedelta(days=offset)
            windows = day_windows(day)
            result.append(
                Partition(
                    source=self.name,
                    dataset=self.dataset,
                    symbol=symbol,
                    period_start=day,
                    period_end=day,
                    file_name=f"{symbol}-candles-1m-{day.isoformat()}.jsonl",
                    # Describes the whole day; it is fetched as day_windows(day), in order.
                    url=self._url(symbol, windows[0][0], windows[-1][1]),
                )
            )
        return result

    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        # Windows are disjoint and every candle is checked against its own window, so the day
        # has no duplicates once each response has none.
        lines = []
        candles = 0
        for start, end in day_windows(partition.period_start):
            body = self._http.get_bytes(self._url(partition.symbol, start, end))
            candles += len(validate_candles(body, start, end))
            lines.append(body + b"\n")
        if not candles:
            raise NotPublished(partition.url)

        data = b"".join(lines)
        dest = dest_dir / partition.file_name
        dest.write_bytes(data)
        return FetchedFile(
            partition=partition,
            local_path=dest,
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            fetched_at=datetime.now(UTC),
        )

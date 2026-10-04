"""FRED series as vintages, from the FRED API (api.stlouisfed.org).

A vintage is a series exactly as FRED published it on one date. FRED lists each series' vintage
dates; each date is one partition, landed as the series' full history requested with
realtime_start = realtime_end = that date. A past vintage never changes, so a landed file is final,
and its date is when those values became knowable (ADR 0004). This is what lets a backtest see the
unemployment rate as first reported instead of as revised months later.

The API key travels in the query string, so it is added only to the URL that is requested. Partition
URLs, manifests and log lines never carry it.
"""

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pitlake_collector.sources.base import (
    FetchedFile,
    Http,
    InvalidResponse,
    Partition,
    Secrets,
    Source,
)

BASE_URL = "https://api.stlouisfed.org/fred"
API_KEY_SECRET = "fred-api-key"
# The API's maximum page sizes. A series that needs more is refused rather than half-landed.
_MAX_VINTAGE_DATES = 10000
_MAX_OBSERVATIONS = 100000

_SERIES = re.compile(r"^[A-Z0-9][A-Z0-9_]{0,29}$")
_API_KEY = re.compile(r"^[a-z0-9]{32}$")
_OBSERVATION_FIELDS = ("realtime_start", "realtime_end", "date", "value")


def _json(body: bytes, what: str):
    if b"\n" in body or b"\r" in body:
        raise InvalidResponse(f"{what}: response body contains a line break")
    try:
        return json.loads(body)
    except ValueError as exc:
        raise InvalidResponse(f"{what}: response is not JSON: {body[:200]!r}") from exc


def _complete(data: dict, items: list, what: str) -> None:
    if data.get("count", len(items)) != len(items):
        raise InvalidResponse(f"{what}: {data.get('count')} items, {len(items)} in the response")


def parse_vintage_dates(body: bytes, start: date, end: date) -> list[date]:
    data = _json(body, "vintage dates")
    dates = data.get("vintage_dates") if isinstance(data, dict) else None
    if not isinstance(dates, list) or not all(isinstance(d, str) for d in dates):
        raise InvalidResponse(f"vintage dates: unexpected response {body[:200]!r}")
    _complete(data, dates, "vintage dates")
    try:
        parsed = [date.fromisoformat(d) for d in dates]
    except ValueError as exc:
        raise InvalidResponse(f"vintage dates: {exc}") from exc
    return sorted({d for d in parsed if start <= d <= end})


def validate_vintage(body: bytes, vintage: date) -> int:
    """Check one vintage's observations and return how many there are. Raises InvalidResponse."""
    what = f"vintage {vintage.isoformat()}"
    data = _json(body, what)
    observations = data.get("observations") if isinstance(data, dict) else None
    if not isinstance(observations, list) or not observations:
        raise InvalidResponse(f"{what}: no observations in {body[:200]!r}")
    _complete(data, observations, what)
    day = vintage.isoformat()
    dates = []
    for obs in observations:
        if not isinstance(obs, dict) or not all(
            isinstance(obs.get(f), str) for f in _OBSERVATION_FIELDS
        ):
            raise InvalidResponse(f"{what}: unexpected observation {obs!r}")
        # ISO dates compare correctly as strings.
        if not obs["realtime_start"] <= day <= obs["realtime_end"]:
            raise InvalidResponse(f"{what}: observation outside the vintage: {obs!r}")
        dates.append(obs["date"])
    if dates != sorted(set(dates)):
        raise InvalidResponse(f"{what}: observation dates are not unique and ascending")
    return len(observations)


class FredVintages(Source):
    name = "fred"
    dataset = "series_vintages"

    def __init__(self, http: Http, api_key: str, base_url: str = BASE_URL) -> None:
        if not _API_KEY.fullmatch(api_key):
            # Never echo the value: it may be a real key pasted with a typo.
            raise ValueError(f"{API_KEY_SECRET} is not 32 lowercase letters and digits")
        self._http = http
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")

    @classmethod
    def create(cls, http: Http, secrets: Secrets) -> "FredVintages":
        return cls(http, api_key=secrets(API_KEY_SECRET))

    def validate_symbol(self, symbol: str) -> str:
        if not _SERIES.fullmatch(symbol):
            raise ValueError(f"invalid FRED series id: {symbol!r}")
        return symbol

    def latest_available(self, today: date) -> date:
        # FRED dates its real-time periods in US time; yesterday is always a finished day there.
        return today - timedelta(days=1)

    def _get(self, url: str) -> bytes:
        return self._http.get_bytes(f"{url}&api_key={self._api_key}")

    def _observations_url(self, series: str, vintage: date) -> str:
        day = vintage.isoformat()
        return (
            f"{self._base_url}/series/observations?series_id={series}"
            f"&realtime_start={day}&realtime_end={day}&file_type=json&limit={_MAX_OBSERVATIONS}"
        )

    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        self.validate_symbol(symbol)
        if end < start:
            return []
        body = self._get(
            f"{self._base_url}/series/vintagedates?series_id={symbol}"
            f"&realtime_start={start.isoformat()}&realtime_end={end.isoformat()}"
            f"&file_type=json&limit={_MAX_VINTAGE_DATES}"
        )
        return [
            Partition(
                source=self.name,
                dataset=self.dataset,
                symbol=symbol,
                period_start=vintage,
                period_end=vintage,
                file_name=f"{symbol}-vintage-{vintage.isoformat()}.jsonl",
                url=self._observations_url(symbol, vintage),
            )
            for vintage in parse_vintage_dates(body, start, end)
        ]

    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        body = self._get(partition.url)
        validate_vintage(body, partition.period_start)
        data = body + b"\n"
        dest = dest_dir / partition.file_name
        dest.write_bytes(data)
        return FetchedFile(
            partition=partition,
            local_path=dest,
            sha256=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            fetched_at=datetime.now(UTC),
        )

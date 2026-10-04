"""US equities from the Alpaca market data API (data.alpaca.markets): trades and daily bars.

One landed file per symbol per trading day, holding the API's responses one per line (ADR 0004).
Trading days come from Alpaca's market calendar, so weekends and holidays are never requested. A
trading day runs from midnight to midnight New York time, which keeps after-hours trades (until
20:00 ET, past midnight UTC) with their session.

Trades come from IEX, the one exchange the free plan streams. Daily bars come from the consolidated
tape (SIP), which the free plan serves for anything older than 15 minutes, and they are requested
unadjusted: split-adjusted history is rewritten after every split, so it is not point-in-time.

Credentials travel in request headers, never in a URL.
"""

import hashlib
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import ClassVar
from urllib.parse import quote
from zoneinfo import ZoneInfo

from pitlake_collector.sources.base import (
    FetchedFile,
    Http,
    InvalidResponse,
    NotPublished,
    Partition,
    Secrets,
    Source,
)

DATA_URL = "https://data.alpaca.markets/v2"
CALENDAR_URL = "https://paper-api.alpaca.markets/v2/calendar"
KEY_ID_SECRET = "alpaca-key-id"
SECRET_KEY_SECRET = "alpaca-secret-key"
NEW_YORK = ZoneInfo("America/New_York")

PAGE_LIMIT = 10000
# A day with more pages than this is refused rather than half-landed; IEX trades for the largest
# US stocks need a handful.
MAX_PAGES = 100

_SYMBOL = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")
_CREDENTIAL = re.compile(r"^[A-Za-z0-9]{10,64}$")
_PAGE_TOKEN = re.compile(r"^[A-Za-z0-9+/=_-]{1,512}$")


def session_window(day: date) -> tuple[datetime, datetime]:
    """[start, end) of one trading day in UTC: midnight to midnight in New York."""
    start = datetime.combine(day, time(), tzinfo=NEW_YORK)
    end = datetime.combine(day + timedelta(days=1), time(), tzinfo=NEW_YORK)
    return start.astimezone(UTC), end.astimezone(UTC)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_before(moment: datetime) -> str:
    """The last nanosecond before `moment`. Alpaca's `end` is inclusive, so ending a day at the next
    midnight would also return the next day's daily bar, which is stamped exactly at midnight."""
    last_second = moment.astimezone(UTC) - timedelta(seconds=1)
    return last_second.strftime("%Y-%m-%dT%H:%M:%S.999999999Z")


def _parse_time(value: object) -> datetime:
    """Alpaca timestamps are RFC 3339 in UTC with up to nanoseconds; seconds are enough here."""
    if not isinstance(value, str) or len(value) < 20 or not value.endswith("Z"):
        raise InvalidResponse(f"unexpected timestamp {value!r}")
    try:
        return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise InvalidResponse(f"unexpected timestamp {value!r}") from exc


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def parse_calendar(body: bytes, start: date, end: date) -> list[date]:
    try:
        days = json.loads(body)
    except ValueError as exc:
        raise InvalidResponse(f"calendar is not JSON: {body[:200]!r}") from exc
    if not isinstance(days, list) or not all(
        isinstance(d, dict) and isinstance(d.get("date"), str) for d in days
    ):
        raise InvalidResponse(f"unexpected calendar response: {body[:200]!r}")
    try:
        parsed = {date.fromisoformat(d["date"]) for d in days}
    except ValueError as exc:
        raise InvalidResponse(f"calendar: {exc}") from exc
    return sorted(d for d in parsed if start <= d <= end)


class AlpacaSource(Source):
    """What trades and bars share: credentials, the calendar, pagination and landing."""

    # Key of the records array in each response, and the URL path after /stocks/<symbol>/.
    records_key: ClassVar[str]
    endpoint: ClassVar[str]
    query: ClassVar[str]
    file_kind: ClassVar[str]

    def __init__(
        self,
        http: Http,
        key_id: str,
        secret_key: str,
        data_url: str = DATA_URL,
        calendar_url: str = CALENDAR_URL,
    ) -> None:
        for name, value in ((KEY_ID_SECRET, key_id), (SECRET_KEY_SECRET, secret_key)):
            if not _CREDENTIAL.fullmatch(value):
                # Never echo the value: it may be a real key pasted with a typo.
                raise ValueError(f"{name} is not 10 to 64 letters and digits")
        self._http = http
        self._headers = {"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret_key}
        self._data_url = data_url.rstrip("/")
        self._calendar_url = calendar_url
        self._calendar: dict[tuple[date, date], list[date]] = {}

    @classmethod
    def create(cls, http: Http, secrets: Secrets) -> "AlpacaSource":
        return cls(http, key_id=secrets(KEY_ID_SECRET), secret_key=secrets(SECRET_KEY_SECRET))

    def validate_symbol(self, symbol: str) -> str:
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError(f"invalid US stock symbol: {symbol!r}")
        return symbol

    def latest_available(self, today: date) -> date:
        # Yesterday's session, after-hours included, has ended in New York by midnight UTC + 5h;
        # the job runs at 06:30 UTC.
        return today - timedelta(days=1)

    def trading_days(self, start: date, end: date) -> list[date]:
        """Asked once per run for all symbols of the feed."""
        if (start, end) not in self._calendar:
            body = self._http.get_bytes(
                f"{self._calendar_url}?start={start.isoformat()}&end={end.isoformat()}",
                self._headers,
            )
            self._calendar[(start, end)] = parse_calendar(body, start, end)
        return self._calendar[(start, end)]

    def _url(self, symbol: str, day: date) -> str:
        start, end = session_window(day)
        return (
            f"{self._data_url}/stocks/{symbol}/{self.endpoint}?{self.query}"
            f"&start={_iso(start)}&end={_iso_before(end)}&limit={PAGE_LIMIT}"
        )

    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        self.validate_symbol(symbol)
        if end < start:
            return []
        return [
            Partition(
                source=self.name,
                dataset=self.dataset,
                symbol=symbol,
                period_start=day,
                period_end=day,
                file_name=f"{symbol}-{self.file_kind}-{day.isoformat()}.jsonl",
                url=self._url(symbol, day),
            )
            for day in self.trading_days(start, end)
        ]

    def validate_page(
        self, body: bytes, symbol: str, day: date, seen: set
    ) -> tuple[int, str | None]:
        """Check one response; return (records, next page token). Raises InvalidResponse."""
        what = f"{symbol} {day.isoformat()}"
        if b"\n" in body or b"\r" in body:
            raise InvalidResponse(f"{what}: response body contains a line break")
        try:
            page = json.loads(body)
        except ValueError as exc:
            raise InvalidResponse(f"{what}: response is not JSON: {body[:200]!r}") from exc
        if not isinstance(page, dict) or page.get("symbol") != symbol:
            raise InvalidResponse(f"{what}: unexpected response {body[:200]!r}")
        records = page.get(self.records_key)
        if records is None:
            records = []
        if not isinstance(records, list):
            raise InvalidResponse(f"{what}: {self.records_key} is not an array")
        start, end = session_window(day)
        for record in records:
            if not isinstance(record, dict) or not start <= _parse_time(record.get("t")) < end:
                raise InvalidResponse(f"{what}: record outside the session: {record!r}")
            self.check_record(record, seen, what)
        token = page.get("next_page_token")
        if token is not None and (not isinstance(token, str) or not _PAGE_TOKEN.fullmatch(token)):
            raise InvalidResponse(f"{what}: unexpected next_page_token {token!r}")
        return len(records), token

    def check_record(self, record: dict, seen: set, what: str) -> None:
        raise NotImplementedError

    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        lines: list[bytes] = []
        seen: set = set()
        records = 0
        token = None
        for _ in range(MAX_PAGES):
            url = (
                partition.url
                if token is None
                else f"{partition.url}&page_token={quote(token, safe='')}"
            )
            body = self._http.get_bytes(url, self._headers)
            count, token = self.validate_page(body, partition.symbol, partition.period_start, seen)
            records += count
            lines.append(body + b"\n")
            if token is None:
                break
        else:
            raise InvalidResponse(f"{partition.file_name}: more than {MAX_PAGES} pages")
        if not records:
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


class AlpacaTrades(AlpacaSource):
    name = "alpaca"
    dataset = "stock_trades"
    records_key = "trades"
    endpoint = "trades"
    query = "feed=iex&sort=asc"
    file_kind = "trades"

    def check_record(self, record: dict, seen: set, what: str) -> None:
        trade_id = record.get("i")
        if not (
            isinstance(trade_id, int)
            and _is_number(record.get("p"))
            and _is_number(record.get("s"))
        ):
            raise InvalidResponse(f"{what}: unexpected trade {record!r}")
        if trade_id in seen:
            raise InvalidResponse(f"{what}: trade {trade_id} served twice")
        seen.add(trade_id)


class AlpacaDailyBars(AlpacaSource):
    name = "alpaca"
    dataset = "stock_bars_1d"
    records_key = "bars"
    endpoint = "bars"
    query = "timeframe=1Day&feed=sip&adjustment=raw"
    file_kind = "bars-1d"

    def check_record(self, record: dict, seen: set, what: str) -> None:
        if not all(_is_number(record.get(f)) for f in ("o", "h", "l", "c", "v")):
            raise InvalidResponse(f"{what}: unexpected bar {record!r}")
        if seen:
            raise InvalidResponse(f"{what}: more than one daily bar")
        seen.add(record["t"])

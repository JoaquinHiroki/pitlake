import json
from datetime import UTC, date, datetime

import pytest

from fakes import FakeHttp
from pitlake_collector.sources.alpaca import (
    CALENDAR_URL,
    DATA_URL,
    AlpacaDailyBars,
    AlpacaTrades,
    parse_calendar,
    session_window,
)
from pitlake_collector.sources.base import InvalidResponse, NotPublished

KEY_ID, SECRET = "PKTESTKEYID0123456789", "s3cretS3cretS3cretS3cretS3cretS3cret0000"
DAY = date(2025, 1, 2)
# 2025-01-09 had no session (national day of mourning); the calendar must skip it.
CALENDAR = [
    {"date": d, "open": "09:30", "close": "16:00"}
    for d in ("2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07", "2025-01-08", "2025-01-10")
]
CALENDAR_REQUEST = f"{CALENDAR_URL}?start=2025-01-01&end=2025-01-10"
CALENDAR_DAY_REQUEST = f"{CALENDAR_URL}?start=2025-01-02&end=2025-01-02"
TRADES_URL = (
    f"{DATA_URL}/stocks/AAPL/trades?feed=iex&sort=asc"
    "&start=2025-01-02T05:00:00Z&end=2025-01-03T04:59:59.999999999Z&limit=10000"
)
BARS_URL = (
    f"{DATA_URL}/stocks/AAPL/bars?timeframe=1Day&feed=sip&adjustment=raw"
    "&start=2025-01-02T05:00:00Z&end=2025-01-03T04:59:59.999999999Z&limit=10000"
)


def _compact(data) -> bytes:
    return json.dumps(data, separators=(",", ":")).encode()


def _trades(ids, token=None, t="2025-01-02T14:30:00.123456789Z") -> bytes:
    trades = [{"t": t, "x": "V", "p": 243.5, "s": 10, "c": ["@"], "i": i, "z": "C"} for i in ids]
    return _compact({"trades": trades, "symbol": "AAPL", "next_page_token": token})


def _bars(*bars) -> bytes:
    return _compact({"bars": list(bars), "symbol": "AAPL", "next_page_token": None})


BAR = {
    "t": "2025-01-02T05:00:00Z",
    "o": 248.9,
    "h": 249.1,
    "l": 241.8,
    "c": 243.85,
    "v": 55740731,
    "n": 769140,
    "vw": 244.8,
}


def _source(cls, files):
    calendar = _compact(CALENDAR)
    http = FakeHttp({CALENDAR_REQUEST: calendar, CALENDAR_DAY_REQUEST: calendar, **files})
    return cls(http, key_id=KEY_ID, secret_key=SECRET), http


def test_session_window_is_new_york_midnight_to_midnight():
    assert session_window(DAY) == (
        datetime(2025, 1, 2, 5, tzinfo=UTC),
        datetime(2025, 1, 3, 5, tzinfo=UTC),
    )
    # Summer time: New York is UTC-4.
    assert session_window(date(2025, 7, 1))[0] == datetime(2025, 7, 1, 4, tzinfo=UTC)


def test_partitions_are_trading_days_only():
    source, http = _source(AlpacaTrades, {})
    parts = source.partitions("AAPL", date(2025, 1, 1), date(2025, 1, 10))
    assert [p.period_start for p in parts] == [date.fromisoformat(d["date"]) for d in CALENDAR]
    assert parts[0].file_name == "AAPL-trades-2025-01-02.jsonl"
    assert parts[0].relpath == "alpaca/stock_trades/AAPL/AAPL-trades-2025-01-02.jsonl"
    assert parts[0].url == TRADES_URL
    # The calendar is asked once, whatever the number of symbols.
    source.partitions("SPY", date(2025, 1, 1), date(2025, 1, 10))
    assert http.requests.count(CALENDAR_REQUEST) == 1


def test_credentials_go_in_headers_only():
    source, http = _source(AlpacaTrades, {TRADES_URL: _trades([1, 2])})
    [part] = source.partitions("AAPL", DAY, DAY)
    assert all(KEY_ID not in url and SECRET not in url for url in http.requests)
    assert all(
        h == {"APCA-API-KEY-ID": KEY_ID, "APCA-API-SECRET-KEY": SECRET} for h in http.headers
    )
    assert KEY_ID not in part.url


def test_trades_follow_every_page(tmp_path):
    page2_url = TRADES_URL + "&page_token=QUFQTHwx%2FMjAyNQ%3D%3D"
    files = {
        TRADES_URL: _trades([1, 2], token="QUFQTHwx/MjAyNQ=="),
        page2_url: _trades([3]),
    }
    source, _ = _source(AlpacaTrades, files)
    [part] = source.partitions("AAPL", DAY, DAY)
    fetched = source.fetch(part, tmp_path)
    assert fetched.local_path.read_bytes().splitlines() == [files[TRADES_URL], files[page2_url]]


def test_a_trade_served_twice_is_rejected(tmp_path):
    page2_url = TRADES_URL + "&page_token=abc"
    source, _ = _source(
        AlpacaTrades, {TRADES_URL: _trades([1, 2], token="abc"), page2_url: _trades([2])}
    )
    [part] = source.partitions("AAPL", DAY, DAY)
    with pytest.raises(InvalidResponse):
        source.fetch(part, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_after_hours_trades_belong_to_their_session(tmp_path):
    # 19:59 New York on 2025-01-02 is 00:59 UTC on 2025-01-03.
    source, _ = _source(AlpacaTrades, {TRADES_URL: _trades([1], t="2025-01-03T00:59:00Z")})
    [part] = source.partitions("AAPL", DAY, DAY)
    assert source.fetch(part, tmp_path).size_bytes > 0


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b'{"message":"forbidden"}',
        _trades([1]).replace(b'"AAPL"', b'"MSFT"'),
        _trades([1], t="2025-01-03T05:00:00Z"),
        _trades([1], t="2025-01-02 14:30:00"),
        _trades([1]).replace(b'"p":243.5', b'"p":"243.5"'),
        _trades([1], token="bad token!"),
        _trades([1]).replace(b"},{", b"},\n{") + b"\n",
    ],
)
def test_invalid_trade_pages_are_rejected(body, tmp_path):
    source, _ = _source(AlpacaTrades, {TRADES_URL: body})
    [part] = source.partitions("AAPL", DAY, DAY)
    with pytest.raises(InvalidResponse):
        source.fetch(part, tmp_path)


def test_a_trading_day_without_trades_is_not_published(tmp_path):
    empty = _compact({"trades": None, "symbol": "AAPL", "next_page_token": None})
    source, _ = _source(AlpacaTrades, {TRADES_URL: empty})
    [part] = source.partitions("AAPL", DAY, DAY)
    with pytest.raises(NotPublished):
        source.fetch(part, tmp_path)


def test_the_next_days_bar_is_never_accepted(tmp_path):
    # Found in dev: with an inclusive end at the next midnight, Alpaca also returned the next
    # trading day's bar, stamped exactly at that midnight.
    next_day = {**BAR, "t": "2025-01-03T05:00:00Z"}
    source, _ = _source(AlpacaDailyBars, {BARS_URL: _bars(BAR, next_day)})
    [part] = source.partitions("AAPL", DAY, DAY)
    assert "&end=2025-01-03T04:59:59.999999999Z" in part.url
    with pytest.raises(InvalidResponse):
        source.fetch(part, tmp_path)


def test_daily_bar_lands(tmp_path):
    source, _ = _source(AlpacaDailyBars, {BARS_URL: _bars(BAR)})
    [part] = source.partitions("AAPL", DAY, DAY)
    assert part.file_name == "AAPL-bars-1d-2025-01-02.jsonl"
    assert part.url == BARS_URL
    assert source.fetch(part, tmp_path).local_path.read_bytes() == _bars(BAR) + b"\n"


@pytest.mark.parametrize(
    "bars", [(BAR, {**BAR, "t": "2025-01-02T06:00:00Z"}), ({**BAR, "c": None},)]
)
def test_invalid_bars_are_rejected(bars, tmp_path):
    source, _ = _source(AlpacaDailyBars, {BARS_URL: _bars(*bars)})
    [part] = source.partitions("AAPL", DAY, DAY)
    with pytest.raises(InvalidResponse):
        source.fetch(part, tmp_path)


def test_calendar_rejects_garbage():
    with pytest.raises(InvalidResponse):
        parse_calendar(b'{"message":"unauthorized"}', DAY, DAY)
    assert parse_calendar(_compact(CALENDAR), date(2025, 1, 3), date(2025, 1, 6)) == [
        date(2025, 1, 3),
        date(2025, 1, 6),
    ]


@pytest.mark.parametrize("symbol", ["aapl", "AAPL1", "TOOLONG", "../X", "BRK-B"])
def test_symbol_validation(symbol):
    source, _ = _source(AlpacaTrades, {})
    with pytest.raises(ValueError):
        source.validate_symbol(symbol)


def test_create_reads_both_credentials():
    asked = []
    values = {"alpaca-key-id": KEY_ID, "alpaca-secret-key": SECRET}
    source = AlpacaDailyBars.create(FakeHttp({}), lambda name: asked.append(name) or values[name])
    assert asked == ["alpaca-key-id", "alpaca-secret-key"]
    assert isinstance(source, AlpacaDailyBars)


def test_malformed_credentials_are_refused_without_echoing_them():
    with pytest.raises(ValueError) as info:
        AlpacaTrades(FakeHttp({}), key_id="PK with spaces", secret_key=SECRET)
    assert "PK with spaces" not in str(info.value)

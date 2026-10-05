import json
from datetime import date

import pytest

from fakes import FakeHttp
from pitlake_collector.sources.base import InvalidResponse
from pitlake_collector.sources.edgar import (
    DATA_URL,
    TICKERS_URL,
    EdgarCompanyFacts,
    EdgarFilings,
    latest_periodic_report,
    parse_recent_filings,
    validate_company_facts,
)

AGENT = "PITLake research contact@example.com"
CIK = 320193
SUBMISSIONS_URL = f"{DATA_URL}/submissions/CIK0000320193.json"
FACTS_URL = f"{DATA_URL}/api/xbrl/companyfacts/CIK0000320193.json"
START, END = date(2025, 1, 1), date(2025, 3, 31)
Q1 = "0000320193-25-000008"


def _compact(data) -> bytes:
    return json.dumps(data, separators=(",", ":")).encode()


TICKERS = _compact(
    {
        "0": {"cik_str": CIK, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
    }
)

# (accession, filing date, accepted, form, isXBRL), newest first as the SEC lists them.
FILINGS = [
    ("0001140361-25-010000", "2025-04-01", "2025-04-01T20:00:00.000Z", "4", 0),
    ("0000320193-25-000057", "2025-05-02", "2025-05-02T10:00:46.000Z", "10-Q", 1),
    ("0000320193-25-000010", "2025-02-20", "2025-02-20T21:00:00.000Z", "8-K", 1),
    (Q1, "2025-01-31", "2025-01-31T11:01:27.000Z", "10-Q", 1),
    ("0000320193-24-000123", "2024-11-01", "2024-11-01T10:01:36.000Z", "10-K", 1),
    ("0000320193-24-000081", "2024-08-02", "2024-08-01T22:03:34.000Z", "10-Q", 1),
]


def _submissions(filings=FILINGS, cik=CIK, **overrides) -> bytes:
    recent = {
        "accessionNumber": [f[0] for f in filings],
        "filingDate": [f[1] for f in filings],
        "reportDate": ["" for _ in filings],
        "acceptanceDateTime": [f[2] for f in filings],
        "form": [f[3] for f in filings],
        "isXBRL": [f[4] for f in filings],
        **overrides,
    }
    return _compact({"cik": f"{cik:010d}", "name": "Apple Inc.", "filings": {"recent": recent}})


def _facts(*accessions: str, cik=CIK) -> bytes:
    records = [
        {
            "start": "2024-09-29",
            "end": "2024-12-28",
            "val": 36330000000,
            "accn": accn,
            "fy": 2025,
            "fp": "Q1",
            "form": "10-Q",
            "filed": "2025-01-31",
        }
        for accn in accessions
    ]
    concept = {"label": "Net Income", "description": "Profit.", "units": {"USD": records}}
    return _compact(
        {"cik": cik, "entityName": "Apple Inc.", "facts": {"us-gaap": {"NetIncomeLoss": concept}}}
    )


def _http(**files: bytes) -> FakeHttp:
    return FakeHttp({TICKERS_URL: TICKERS, SUBMISSIONS_URL: _submissions(), **files})


def test_one_partition_for_the_newest_periodic_report_in_the_window():
    source = EdgarCompanyFacts(_http(), user_agent=AGENT)
    [partition] = source.partitions("AAPL", START, END)
    assert partition.file_name == f"AAPL-companyfacts-{Q1}.jsonl"
    assert partition.relpath == f"edgar/company_facts/AAPL/AAPL-companyfacts-{Q1}.jsonl"
    assert partition.period_start == partition.period_end == date(2025, 1, 31)
    assert partition.url == FACTS_URL


def test_filings_partition_points_at_the_submissions_snapshot():
    [partition] = EdgarFilings(_http(), user_agent=AGENT).partitions("AAPL", START, END)
    assert partition.file_name == f"AAPL-submissions-{Q1}.jsonl"
    assert partition.url == SUBMISSIONS_URL


def test_no_periodic_report_in_the_window_means_no_partition():
    source = EdgarCompanyFacts(_http(), user_agent=AGENT)
    assert source.partitions("AAPL", date(2025, 2, 1), date(2025, 3, 31)) == []


def test_a_window_older_than_the_recent_filings_is_refused_only_without_a_report():
    recent = parse_recent_filings(_submissions(FILINGS[:4]), CIK)
    assert latest_periodic_report(recent, date(2024, 1, 1), END, CIK) == (Q1, date(2025, 1, 31))
    with pytest.raises(InvalidResponse):
        latest_periodic_report(recent, date(2024, 1, 1), date(2025, 1, 30), CIK)


def test_non_xbrl_and_non_periodic_filings_do_not_trigger():
    filings = [
        (Q1, "2025-01-31", "x", "10-Q", 0),
        ("0000320193-25-000010", "2025-02-20", "x", "8-K", 1),
    ]
    recent = parse_recent_filings(_submissions(filings), CIK)
    assert latest_periodic_report(recent, date(2025, 1, 31), END, CIK) is None


def test_fetch_lands_the_snapshot_and_identifies_itself(tmp_path):
    body = _facts("0000320193-24-000123", Q1)
    http = _http(**{FACTS_URL: body})
    source = EdgarCompanyFacts(http, user_agent=AGENT)
    [partition] = source.partitions("AAPL", START, END)
    fetched = source.fetch(partition, tmp_path)
    assert fetched.local_path.read_bytes() == body + b"\n"
    assert all(h == {"User-Agent": AGENT} for h in http.headers)
    # The ticker list is asked for once, however many partitions follow.
    assert http.requests.count(TICKERS_URL) == 1


def test_a_snapshot_without_the_triggering_report_is_not_landed(tmp_path):
    source = EdgarCompanyFacts(_http(**{FACTS_URL: _facts("0000320193-24-000123")}), AGENT)
    [partition] = source.partitions("AAPL", START, END)
    with pytest.raises(InvalidResponse, match="not in the snapshot yet"):
        source.fetch(partition, tmp_path)


def test_fetch_filings_lands_the_submissions_body(tmp_path):
    source = EdgarFilings(_http(), user_agent=AGENT)
    [partition] = source.partitions("AAPL", START, END)
    assert source.fetch(partition, tmp_path).local_path.read_bytes() == _submissions() + b"\n"


@pytest.mark.parametrize(
    "body",
    [
        b"<html>",
        _facts(Q1, cik=789019),
        _compact({"cik": CIK, "facts": {}}),
        _facts(Q1).replace(b'"filed":"2025-01-31"', b'"filed":"01/31/2025"'),
        _facts(Q1).replace(b',"accn"', b',"accession"'),
        _facts(Q1).replace(b'"label"', b'\n"label"'),
    ],
)
def test_validate_company_facts_rejects(body):
    with pytest.raises(InvalidResponse):
        validate_company_facts(body, CIK, Q1)


def test_validate_company_facts_counts_facts():
    assert validate_company_facts(_facts("0000320193-24-000123", Q1), CIK, Q1) == 2


@pytest.mark.parametrize(
    "body",
    [
        _submissions(cik=789019),
        _submissions(form=["10-Q"]),
        _submissions(accessionNumber=[Q1] * len(FILINGS)),
        _submissions(filingDate=["31/01/2025"] * len(FILINGS)),
        _compact({"cik": "0000320193", "filings": {}}),
    ],
)
def test_parse_recent_filings_rejects(body):
    with pytest.raises(InvalidResponse):
        parse_recent_filings(body, CIK)


def test_unknown_ticker_fails_the_listing():
    with pytest.raises(InvalidResponse):
        EdgarCompanyFacts(_http(), user_agent=AGENT).partitions("NVDA", START, END)


@pytest.mark.parametrize("bad", ["", "python-requests/2.32", "PITLake\ncontact@example.com"])
def test_user_agent_must_name_a_contact(bad):
    with pytest.raises(ValueError):
        EdgarCompanyFacts(FakeHttp({}), user_agent=bad)


@pytest.mark.parametrize("symbol", ["AAPL", "BRK-B", "BF.B"])
def test_tickers(symbol):
    assert EdgarCompanyFacts(FakeHttp({}), AGENT).validate_symbol(symbol) == symbol


def test_create_reads_the_user_agent_from_secrets():
    asked = []
    source = EdgarFilings.create(FakeHttp({}), lambda name: asked.append(name) or AGENT)
    assert asked == ["sec-user-agent"] and isinstance(source, EdgarFilings)

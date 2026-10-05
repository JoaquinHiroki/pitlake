"""SEC EDGAR company financial statements from the public XBRL APIs (data.sec.gov).

Two datasets per company, both landed as one API response per file (ADR 0004):

  edgar.company_facts  every XBRL fact the company has reported (companyfacts). Each fact carries
                       the accession number and filing date of the filing that reported it, and a
                       value re-reported or restated in a later filing appears again under that
                       filing, so one snapshot holds the full point-in-time history.
  edgar.filings        the company's filing index (submissions), which adds each filing's exact
                       acceptance time; `filed` in companyfacts is only a date.

The SEC serves these as live snapshots, not as files per period, so a partition is "the snapshot
after periodic report X": the newest 10-K or 10-Q (or amendment) in the window. A new report makes a
new partition, and the snapshot must contain that report before it is landed, so a run that comes
before the SEC has processed the filing tries again next time.

The SEC asks every client to identify itself with a User-Agent naming a contact. It is read from the
secret `sec-user-agent` so that no contact address is committed to the repository.
"""

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import ClassVar

from pitlake_collector.sources.base import (
    FetchedFile,
    Http,
    InvalidResponse,
    Partition,
    Secrets,
    Source,
)

DATA_URL = "https://data.sec.gov"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT_SECRET = "sec-user-agent"

# Periodic reports, whose financial statements are what companyfacts holds. 8-Ks and ownership
# forms change the filing index daily and would trigger a snapshot that adds nothing.
PERIODIC_FORMS = frozenset(
    f + amendment
    for f in ("10-K", "10-Q", "10-KT", "10-QT", "20-F", "40-F")
    for amendment in ("", "/A")
)

_SYMBOL = re.compile(r"^[A-Z]{1,5}([.-][A-Z]{1,2})?$")
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_ACCESSION_IN_NAME = re.compile(r"-(\d{10}-\d{2}-\d{6})\.jsonl$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# "Company Name contact@example.com", per https://www.sec.gov/os/accessing-edgar-data.
_USER_AGENT = re.compile(r"^[\x20-\x7e]{3,200}$")
# The filing index fields the trigger and the Bronze reader rely on.
_FILING_FIELDS = ("accessionNumber", "filingDate", "acceptanceDateTime", "form", "isXBRL")
_FACT_FIELDS = ("accn", "filed", "form", "end", "val")


def _json(body: bytes, what: str):
    if b"\n" in body or b"\r" in body:
        raise InvalidResponse(f"{what}: response body contains a line break")
    try:
        return json.loads(body)
    except ValueError as exc:
        raise InvalidResponse(f"{what}: response is not JSON: {body[:200]!r}") from exc


def parse_tickers(body: bytes) -> dict[str, int]:
    """company_tickers.json: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": ...}, ...}."""
    data = _json(body, "company tickers")
    if not isinstance(data, dict) or not data:
        raise InvalidResponse(f"company tickers: unexpected response {body[:200]!r}")
    ciks: dict[str, int] = {}
    for entry in data.values():
        if not (
            isinstance(entry, dict)
            and isinstance(entry.get("ticker"), str)
            and isinstance(entry.get("cik_str"), int)
        ):
            raise InvalidResponse(f"company tickers: unexpected entry {entry!r}")
        # A few tickers map to several CIKs (share classes); the first listed is the issuer.
        ciks.setdefault(entry["ticker"].upper(), entry["cik_str"])
    return ciks


def parse_recent_filings(body: bytes, cik: int) -> dict[str, list]:
    """The column arrays of filings.recent, checked to be complete and consistent."""
    what = f"submissions CIK {cik}"
    data = _json(body, what)
    if not isinstance(data, dict) or str(data.get("cik", "")).lstrip("0") != str(cik):
        raise InvalidResponse(f"{what}: unexpected response {body[:200]!r}")
    recent = (data.get("filings") or {}).get("recent")
    if not isinstance(recent, dict) or not all(
        isinstance(recent.get(f), list) for f in _FILING_FIELDS
    ):
        raise InvalidResponse(f"{what}: filings.recent lacks {_FILING_FIELDS}")
    lengths = {len(v) for v in recent.values() if isinstance(v, list)}
    if len(lengths) != 1 or any(not isinstance(v, list) for v in recent.values()):
        raise InvalidResponse(f"{what}: filings.recent columns differ in length")
    accessions = recent["accessionNumber"]
    if not all(isinstance(a, str) and _ACCESSION.fullmatch(a) for a in accessions):
        raise InvalidResponse(f"{what}: malformed accession number")
    if len(set(accessions)) != len(accessions):
        raise InvalidResponse(f"{what}: an accession number is listed twice")
    if not all(isinstance(d, str) and _DATE.fullmatch(d) for d in recent["filingDate"]):
        raise InvalidResponse(f"{what}: malformed filing date")
    return recent


def latest_periodic_report(
    recent: dict[str, list], start: date, end: date, cik: int
) -> tuple[str, date] | None:
    """(accession, filing date) of the newest periodic XBRL report filed in [start, end]."""
    filed = [date.fromisoformat(d) for d in recent["filingDate"]]
    candidates = [
        (day, accession)
        for accession, day, form, xbrl in zip(
            recent["accessionNumber"], filed, recent["form"], recent["isXBRL"], strict=True
        )
        if form in PERIODIC_FORMS and xbrl == 1 and start <= day <= end
    ]
    if not candidates:
        if filed and min(filed) > start:
            # The SEC moves filings out of `recent` after about 1,000; older ones are in pages this
            # source does not read, so the report this window needs may be among them.
            raise InvalidResponse(
                f"submissions CIK {cik}: no periodic report since {min(filed)}, where recent "
                f"filings start, and the window starts {start}"
            )
        return None
    day, accession = max(candidates)
    return accession, day


def validate_company_facts(body: bytes, cik: int, accession: str) -> int:
    """Check a companyfacts snapshot and return its fact count. Raises InvalidResponse."""
    what = f"companyfacts CIK {cik}"
    data = _json(body, what)
    if not isinstance(data, dict) or data.get("cik") != cik:
        raise InvalidResponse(f"{what}: unexpected response {body[:200]!r}")
    facts = data.get("facts")
    if not isinstance(facts, dict) or not facts:
        raise InvalidResponse(f"{what}: no facts")
    count = 0
    found = False
    for taxonomy, concepts in facts.items():
        if not isinstance(concepts, dict):
            raise InvalidResponse(f"{what}: {taxonomy} is not an object")
        for concept, entry in concepts.items():
            units = entry.get("units") if isinstance(entry, dict) else None
            if not isinstance(units, dict):
                raise InvalidResponse(f"{what}: {taxonomy}:{concept} has no units")
            for unit, records in units.items():
                if not isinstance(records, list):
                    raise InvalidResponse(f"{what}: {taxonomy}:{concept} {unit} is not an array")
                for record in records:
                    if not isinstance(record, dict) or not all(f in record for f in _FACT_FIELDS):
                        raise InvalidResponse(f"{what}: unexpected fact {record!r}")
                    if not (isinstance(record["filed"], str) and _DATE.fullmatch(record["filed"])):
                        raise InvalidResponse(f"{what}: unexpected filing date {record!r}")
                    found = found or record["accn"] == accession
                    count += 1
    if not found:
        raise InvalidResponse(f"{what}: {accession} is not in the snapshot yet")
    return count


class EdgarSource(Source):
    """What both datasets share: the User-Agent, ticker lookup and the periodic-report trigger."""

    name = "edgar"
    file_kind: ClassVar[str]

    def __init__(
        self,
        http: Http,
        user_agent: str,
        data_url: str = DATA_URL,
        tickers_url: str = TICKERS_URL,
    ) -> None:
        if not _USER_AGENT.fullmatch(user_agent) or "@" not in user_agent:
            raise ValueError(f"{USER_AGENT_SECRET} must be a name and a contact email address")
        self._http = http
        self._headers = {"User-Agent": user_agent}
        self._data_url = data_url.rstrip("/")
        self._tickers_url = tickers_url
        self._ciks: dict[str, int] | None = None

    @classmethod
    def create(cls, http: Http, secrets: Secrets) -> "EdgarSource":
        return cls(http, user_agent=secrets(USER_AGENT_SECRET))

    def _get(self, url: str) -> bytes:
        return self._http.get_bytes(url, self._headers)

    def validate_symbol(self, symbol: str) -> str:
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError(f"invalid ticker: {symbol!r}")
        return symbol

    def latest_available(self, today: date) -> date:
        # The snapshots are live: a report accepted today is in them within minutes.
        return today

    def cik(self, symbol: str) -> int:
        """Asked once per run for all symbols of the feed."""
        if self._ciks is None:
            self._ciks = parse_tickers(self._get(self._tickers_url))
        try:
            return self._ciks[symbol]
        except KeyError:
            raise InvalidResponse(f"{symbol} is not in the SEC ticker list") from None

    def _submissions_url(self, cik: int) -> str:
        return f"{self._data_url}/submissions/CIK{cik:010d}.json"

    def snapshot_url(self, cik: int) -> str:
        raise NotImplementedError

    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        self.validate_symbol(symbol)
        if end < start:
            return []
        cik = self.cik(symbol)
        recent = parse_recent_filings(self._get(self._submissions_url(cik)), cik)
        latest = latest_periodic_report(recent, start, end, cik)
        if latest is None:
            return []
        accession, filed = latest
        return [
            Partition(
                source=self.name,
                dataset=self.dataset,
                symbol=symbol,
                period_start=filed,
                period_end=filed,
                file_name=f"{symbol}-{self.file_kind}-{accession}.jsonl",
                url=self.snapshot_url(cik),
            )
        ]

    def validate(self, body: bytes, cik: int, accession: str) -> None:
        raise NotImplementedError

    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        cik = self.cik(partition.symbol)
        accession = _ACCESSION_IN_NAME.search(partition.file_name).group(1)
        body = self._get(partition.url)
        self.validate(body, cik, accession)
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


class EdgarCompanyFacts(EdgarSource):
    dataset = "company_facts"
    file_kind = "companyfacts"

    def snapshot_url(self, cik: int) -> str:
        return f"{self._data_url}/api/xbrl/companyfacts/CIK{cik:010d}.json"

    def validate(self, body: bytes, cik: int, accession: str) -> None:
        validate_company_facts(body, cik, accession)


class EdgarFilings(EdgarSource):
    dataset = "filings"
    file_kind = "submissions"

    def snapshot_url(self, cik: int) -> str:
        return self._submissions_url(cik)

    def validate(self, body: bytes, cik: int, accession: str) -> None:
        if accession not in parse_recent_filings(body, cik)["accessionNumber"]:
            raise InvalidResponse(f"submissions CIK {cik}: {accession} is not listed")

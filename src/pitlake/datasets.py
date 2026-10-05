"""Registry of datasets the platform knows how to load.

The Bronze loader is generic: everything source-specific it needs lives in a Dataset entry,
so adding a source means adding an entry here, not a branch in the job.
"""

import re
from dataclasses import dataclass

from pitlake.config import validate_identifier

# How a landed file is wrapped: "zip" holds exactly one data file, "none" is the data file itself.
ARCHIVES = ("zip", "none")
# How the data file is read (ADR 0004). The JSON formats are JSON Lines, one API response per line:
# "json_rows": the response is an array of rows, each an array of values in `columns` order.
# "json_records": the response is an object whose `records_path` key holds an array of objects;
#                 `fields` are the keys read from each object.
# "json_columns": the response is an object whose `records_path` (dotted for nested objects) holds
#                 one array per field, all of the same length; record i is element i of each.
# "xbrl_facts": the response is an XBRL facts document, facts -> taxonomy -> concept -> units ->
#               unit -> array of objects (the SEC's companyfacts). The first three columns receive
#               the taxonomy, concept and unit; `fields` after them are read from each object.
FILE_FORMATS = ("csv", "json_rows", "json_records", "json_columns", "xbrl_facts")
XBRL_PATH_COLUMNS = ("taxonomy", "concept", "unit")
_RECORDS_PATH_FORMATS = ("json_records", "json_columns")
# JSON keys as the source spells them, e.g. accessionNumber. Bronze columns stay lowercase.
_JSON_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


@dataclass(frozen=True)
class Dataset:
    source: str
    name: str
    # Column names of the raw file in order. Bronze keeps every value as STRING;
    # typing happens in Silver, so a malformed value never fails the load.
    columns: tuple[str, ...]
    archive: str = "zip"
    file_format: str = "csv"
    header: bool = False
    records_path: str = ""
    # JSON keys read for `columns`, in the same order, when the source's names are not valid column
    # names (camelCase). Empty means the keys are the column names.
    source_fields: tuple[str, ...] = ()
    # Files per Delta commit in load_bronze. A commit has a fixed cost of several seconds, so
    # datasets of many small files batch more of them; Binance's daily files hold millions of rows.
    files_per_commit: int = 5
    description: str = ""

    def __post_init__(self) -> None:
        validate_identifier(self.source)
        validate_identifier(self.name)
        if self.archive not in ARCHIVES:
            raise ValueError(f"{self.archive!r}: archive must be one of {ARCHIVES}")
        if self.file_format not in FILE_FORMATS:
            raise ValueError(f"{self.file_format!r}: file_format must be one of {FILE_FORMATS}")
        if (self.file_format in _RECORDS_PATH_FORMATS) != bool(self.records_path):
            raise ValueError(f"records_path is required for {_RECORDS_PATH_FORMATS} and only them")
        for key in self.records_path.split(".") if self.records_path else ():
            if not _JSON_KEY.fullmatch(key):
                raise ValueError(f"{self.records_path!r}: invalid records_path")
        if self.source_fields:
            if len(self.source_fields) != len(self.columns):
                raise ValueError("source_fields must name one key per column")
            for key in self.source_fields:
                if not _JSON_KEY.fullmatch(key):
                    raise ValueError(f"{key!r}: invalid source field")
        if self.file_format == "xbrl_facts" and self.columns[:3] != XBRL_PATH_COLUMNS:
            raise ValueError(f"xbrl_facts columns must start with {XBRL_PATH_COLUMNS}")
        if self.files_per_commit < 1:
            raise ValueError("files_per_commit must be at least 1")
        for column in self.columns:
            validate_identifier(column)
            if column.startswith("_"):
                raise ValueError(f"{column!r}: leading underscore is reserved for metadata")

    @property
    def fields(self) -> tuple[str, ...]:
        """The JSON key read for each column."""
        return self.source_fields or self.columns

    @property
    def key(self) -> str:
        return f"{self.source}.{self.name}"

    @property
    def bronze_table(self) -> str:
        return f"{self.source}_{self.name}"


BINANCE_SPOT_TRADES = Dataset(
    source="binance",
    name="spot_trades",
    # https://github.com/binance/binance-public-data#trades. The time column is milliseconds
    # before 2025-01-01 and microseconds from then on; Bronze keeps it as received.
    columns=(
        "trade_id",
        "price",
        "qty",
        "quote_qty",
        "time",
        "is_buyer_maker",
        "is_best_match",
    ),
    description="Every executed spot trade, one daily file per trading pair.",
)

COINBASE_SPOT_CANDLES_1M = Dataset(
    source="coinbase",
    name="spot_candles_1m",
    # https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles.
    # time is the minute's start in epoch seconds. The API's order is low, high, open, close.
    columns=("time", "low", "high", "open", "close", "volume"),
    archive="none",
    file_format="json_rows",
    files_per_commit=50,
    description="One-minute candles per product, one daily file of API responses (ADR 0004).",
)

FRED_SERIES_VINTAGES = Dataset(
    source="fred",
    name="series_vintages",
    # https://fred.stlouisfed.org/docs/api/fred/series_observations.html. One landed file is one
    # vintage: the whole series as published on _period_start. Missing values arrive as ".".
    columns=("realtime_start", "realtime_end", "date", "value"),
    archive="none",
    file_format="json_records",
    records_path="observations",
    files_per_commit=50,
    description="Every published vintage of each series, one file per vintage date (ADR 0004).",
)

ALPACA_STOCK_TRADES = Dataset(
    source="alpaca",
    name="stock_trades",
    # https://docs.alpaca.markets/reference/stocktrades. t: RFC 3339 time, x: exchange, p: price,
    # s: size, c: condition codes (kept as their JSON text), i: trade id, z: tape.
    columns=("t", "x", "p", "s", "c", "i", "z"),
    archive="none",
    file_format="json_records",
    records_path="trades",
    files_per_commit=20,
    description="IEX trades per symbol, one file per New York trading day (ADR 0004).",
)

ALPACA_STOCK_BARS_1D = Dataset(
    source="alpaca",
    name="stock_bars_1d",
    # https://docs.alpaca.markets/reference/stockbars. Consolidated (SIP), unadjusted. o/h/l/c,
    # v: volume, n: trade count, vw: volume-weighted average price.
    columns=("t", "o", "h", "l", "c", "v", "n", "vw"),
    archive="none",
    file_format="json_records",
    records_path="bars",
    files_per_commit=50,
    description="Unadjusted daily bars per symbol, one file per trading day (ADR 0004).",
)

EDGAR_COMPANY_FACTS = Dataset(
    source="edgar",
    name="company_facts",
    # https://www.sec.gov/edgar/sec-api-documentation. One landed file is one companyfacts snapshot.
    # accn and filed identify the filing that reported the value; a restated value is a new row.
    # start is absent for instant facts (balances), frame for values the SEC did not align.
    columns=(
        *XBRL_PATH_COLUMNS,
        "start",
        "end",
        "val",
        "accn",
        "fy",
        "fp",
        "form",
        "filed",
        "frame",
    ),
    archive="none",
    file_format="xbrl_facts",
    files_per_commit=10,
    description="Every XBRL fact each company has filed, one snapshot per periodic report "
    "(ADR 0004).",
)

EDGAR_FILINGS = Dataset(
    source="edgar",
    name="filings",
    # The submissions API's filings.recent. acceptance_date_time is when the SEC accepted the filing
    # (UTC), which can be the evening before filing_date.
    columns=(
        "accession_number",
        "filing_date",
        "report_date",
        "acceptance_date_time",
        "act",
        "form",
        "file_number",
        "film_number",
        "items",
        "size",
        "is_xbrl",
        "is_inline_xbrl",
        "primary_document",
        "primary_doc_description",
    ),
    source_fields=(
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "act",
        "form",
        "fileNumber",
        "filmNumber",
        "items",
        "size",
        "isXBRL",
        "isInlineXBRL",
        "primaryDocument",
        "primaryDocDescription",
    ),
    archive="none",
    file_format="json_columns",
    records_path="filings.recent",
    files_per_commit=10,
    description="Each company's filing index with acceptance times, one snapshot per periodic "
    "report (ADR 0004).",
)

DATASETS: dict[str, Dataset] = {
    d.key: d
    for d in (
        BINANCE_SPOT_TRADES,
        COINBASE_SPOT_CANDLES_1M,
        FRED_SERIES_VINTAGES,
        ALPACA_STOCK_TRADES,
        ALPACA_STOCK_BARS_1D,
        EDGAR_COMPANY_FACTS,
        EDGAR_FILINGS,
    )
}


def get_dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise ValueError(f"unknown dataset {key!r}; known: {sorted(DATASETS)}") from None

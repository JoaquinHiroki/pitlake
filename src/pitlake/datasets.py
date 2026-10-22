import re
from dataclasses import dataclass

from pitlake.config import validate_identifier

ARCHIVES = ("zip", "none")
FILE_FORMATS = ("csv", "json_rows", "json_records", "json_columns", "xbrl_facts", "document")
XBRL_PATH_COLUMNS = ("taxonomy", "concept", "unit")
_RECORDS_PATH_FORMATS = ("json_records", "json_columns")
_JSON_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


@dataclass(frozen=True)
class Dataset:
    source: str
    name: str
    columns: tuple[str, ...]
    archive: str = "zip"
    file_format: str = "csv"
    header: bool = False
    records_path: str = ""
    source_fields: tuple[str, ...] = ()
    files_per_commit: int = 5
    stale_after_days: int = 3
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
        if self.file_format == "document" and len(self.columns) != 1:
            raise ValueError("a document dataset has exactly one column, the whole file")
        if self.file_format == "xbrl_facts" and self.columns[:3] != XBRL_PATH_COLUMNS:
            raise ValueError(f"xbrl_facts columns must start with {XBRL_PATH_COLUMNS}")
        if self.files_per_commit < 1:
            raise ValueError("files_per_commit must be at least 1")
        if self.stale_after_days < 1:
            raise ValueError("stale_after_days must be at least 1")
        for column in self.columns:
            validate_identifier(column)
            if column.startswith("_"):
                raise ValueError(f"{column!r}: leading underscore is reserved for metadata")

    @property
    def fields(self) -> tuple[str, ...]:
        return self.source_fields or self.columns

    @property
    def key(self) -> str:
        return f"{self.source}.{self.name}"

    @property
    def bronze_table(self) -> str:
        return f"{self.source}_{self.name}"


# Binance trade times are milliseconds before 2025-01-01 and microseconds after; Silver converts.
BINANCE_SPOT_TRADES = Dataset(
    source="binance",
    name="spot_trades",
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
    columns=("time", "low", "high", "open", "close", "volume"),
    archive="none",
    file_format="json_rows",
    files_per_commit=50,
    description="One-minute candles per product, one daily file of API responses (ADR 0004).",
)

FRED_SERIES_VINTAGES = Dataset(
    source="fred",
    name="series_vintages",
    stale_after_days=30,
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
    stale_after_days=6,
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
    stale_after_days=6,
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
    stale_after_days=100,
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
    stale_after_days=45,
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

EDGAR_FILING_DOCUMENTS = Dataset(
    source="edgar",
    name="filing_documents",
    stale_after_days=100,
    columns=("document",),
    archive="none",
    file_format="document",
    files_per_commit=10,
    description="The primary HTML document of each 10-K and 10-Q, one file per filing as "
    "received (ADR 0010).",
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
        EDGAR_FILING_DOCUMENTS,
    )
}


def get_dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise ValueError(f"unknown dataset {key!r}; known: {sorted(DATASETS)}") from None

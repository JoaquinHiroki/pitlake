"""Registry of datasets the platform knows how to load.

The Bronze loader is generic: everything source-specific it needs lives in a Dataset entry,
so adding a source means adding an entry here, not a branch in the job.
"""

from dataclasses import dataclass

from pitlake.config import validate_identifier

# How a landed file is wrapped: "zip" holds exactly one data file, "none" is the data file itself.
ARCHIVES = ("zip", "none")
# How the data file is read (ADR 0004). Both JSON formats are JSON Lines, one API response per line:
# "json_rows": the response is an array of rows, each an array of values in `columns` order.
# "json_records": the response is an object whose `records_path` key holds an array of objects;
#                 `columns` are the keys read from each object.
FILE_FORMATS = ("csv", "json_rows", "json_records")


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
        if (self.file_format == "json_records") != bool(self.records_path):
            raise ValueError("records_path is required for json_records and only for it")
        if self.records_path:
            validate_identifier(self.records_path)
        if self.files_per_commit < 1:
            raise ValueError("files_per_commit must be at least 1")
        for column in self.columns:
            validate_identifier(column)
            if column.startswith("_"):
                raise ValueError(f"{column!r}: leading underscore is reserved for metadata")

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

DATASETS: dict[str, Dataset] = {
    d.key: d
    for d in (
        BINANCE_SPOT_TRADES,
        COINBASE_SPOT_CANDLES_1M,
        FRED_SERIES_VINTAGES,
        ALPACA_STOCK_TRADES,
        ALPACA_STOCK_BARS_1D,
    )
}


def get_dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise ValueError(f"unknown dataset {key!r}; known: {sorted(DATASETS)}") from None

"""Registry of datasets the platform knows how to load.

The Bronze loader is generic: everything source-specific it needs lives in a Dataset entry,
so adding a source means adding an entry here, not a branch in the job.
"""

from dataclasses import dataclass

from pitlake.config import validate_identifier

# How a landed file is wrapped: "zip" holds exactly one data file, "none" is the data file itself.
ARCHIVES = ("zip", "none")
# How the data file is read. "json_rows" is JSON Lines where each line is one API response: an
# array of rows, each row an array of values in `columns` order (ADR 0004).
FILE_FORMATS = ("csv", "json_rows")


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
    description: str = ""

    def __post_init__(self) -> None:
        validate_identifier(self.source)
        validate_identifier(self.name)
        if self.archive not in ARCHIVES:
            raise ValueError(f"{self.archive!r}: archive must be one of {ARCHIVES}")
        if self.file_format not in FILE_FORMATS:
            raise ValueError(f"{self.file_format!r}: file_format must be one of {FILE_FORMATS}")
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
    description="One-minute candles per product, one daily file of API responses (ADR 0004).",
)

DATASETS: dict[str, Dataset] = {d.key: d for d in (BINANCE_SPOT_TRADES, COINBASE_SPOT_CANDLES_1M)}


def get_dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise ValueError(f"unknown dataset {key!r}; known: {sorted(DATASETS)}") from None

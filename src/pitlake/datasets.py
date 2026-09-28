"""Registry of datasets the platform knows how to load.

The Bronze loader is generic: everything source-specific it needs lives in a Dataset entry,
so adding a source means adding an entry here, not a branch in the job.
"""

from dataclasses import dataclass

from pitlake.config import validate_identifier


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

DATASETS: dict[str, Dataset] = {d.key: d for d in (BINANCE_SPOT_TRADES,)}


def get_dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise ValueError(f"unknown dataset {key!r}; known: {sorted(DATASETS)}") from None

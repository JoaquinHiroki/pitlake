"""The landing manifest: the contract between the collector and the platform.

The collector writes one manifest per landed file, after the file itself. The platform
loads manifests into control.landing_manifest and only ever processes files that have one.
Changing a field here changes both sides, so SPARK_SCHEMA must list exactly the same fields.
"""

import json
from dataclasses import asdict, dataclass, fields
from datetime import UTC, date, datetime

SCHEMA_VERSION = 1

SPARK_SCHEMA = (
    "schema_version INT, "
    "source STRING, "
    "dataset STRING, "
    "symbol STRING, "
    "period_start DATE, "
    "period_end DATE, "
    "file_name STRING, "
    "landing_path STRING, "
    "source_url STRING, "
    "sha256 STRING, "
    "size_bytes BIGINT, "
    "fetched_at TIMESTAMP, "
    "collector_version STRING, "
    "collector_host STRING"
)


@dataclass(frozen=True)
class LandingManifest:
    source: str
    dataset: str
    # Instrument, series or company identifier, depending on the source.
    symbol: str
    # Inclusive range of event dates the file covers.
    period_start: date
    period_end: date
    file_name: str
    # Relative to the landing volume root, e.g. binance/spot_trades/BTCUSDT/<file>.
    landing_path: str
    source_url: str
    sha256: str
    size_bytes: int
    fetched_at: datetime
    collector_version: str
    collector_host: str
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> str:
        """One line of JSON, in the formats Spark's JSON reader parses by default."""
        record = asdict(self)
        record["period_start"] = self.period_start.isoformat()
        record["period_end"] = self.period_end.isoformat()
        record["fetched_at"] = _format_timestamp(self.fetched_at)
        return json.dumps(record, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> "LandingManifest":
        record = json.loads(text)
        record["period_start"] = date.fromisoformat(record["period_start"])
        record["period_end"] = date.fromisoformat(record["period_end"])
        record["fetched_at"] = datetime.fromisoformat(record["fetched_at"])
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in record.items() if k in known})


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")
    utc = value.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"

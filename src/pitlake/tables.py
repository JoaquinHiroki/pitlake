"""DDL for the tables the jobs own.

Schemas and volumes are bundle resources; tables are created by the jobs that write them,
idempotently, so a fresh catalog needs nothing but a deploy and a run.
"""

from pitlake.config import table_fqn
from pitlake.datasets import Dataset
from pitlake.manifest import SPARK_SCHEMA as MANIFEST_SPARK_SCHEMA

LANDING_MANIFEST = "landing_manifest"
BRONZE_LOAD_LOG = "bronze_load_log"

CORRUPT_RECORD_COLUMN = "_corrupt_record"

BRONZE_METADATA_COLUMNS = (
    ("_source", "STRING"),
    ("_dataset", "STRING"),
    ("_symbol", "STRING"),
    ("_period_start", "DATE"),
    ("_source_file", "STRING"),
    ("_source_sha256", "STRING"),
    ("_ingested_at", "TIMESTAMP"),
    ("_load_run_id", "STRING"),
)

BRONZE_LOAD_LOG_SCHEMA = (
    "landing_path STRING, sha256 STRING, source STRING, dataset STRING, symbol STRING, "
    "period_start DATE, row_count BIGINT, corrupt_row_count BIGINT, "
    "loaded_at TIMESTAMP, run_id STRING"
)


def landing_manifest_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', LANDING_MANIFEST)} "
        f"({MANIFEST_SPARK_SCHEMA}, _manifest_file STRING, _discovered_at TIMESTAMP) "
        "COMMENT 'One row per manifest the collector wrote. Append-only; a re-fetched file "
        "appears again with its new checksum.'"
    )


def bronze_load_log_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', BRONZE_LOAD_LOG)} "
        f"({BRONZE_LOAD_LOG_SCHEMA}) "
        "COMMENT 'Landed files loaded into Bronze, keyed by landing_path and sha256.'"
    )


def raw_schema(dataset: Dataset) -> str:
    """Every raw column as STRING plus a slot for malformed records, whatever the file format."""
    # Quoted: source column names such as `end` are SQL keywords.
    columns = [f"`{c}` STRING" for c in dataset.columns]
    return ", ".join([*columns, f"{CORRUPT_RECORD_COLUMN} STRING"])


def bronze_ddl(catalog: str, dataset: Dataset) -> str:
    metadata = [f"{name} {kind}" for name, kind in BRONZE_METADATA_COLUMNS]
    columns = ", ".join([raw_schema(dataset), *metadata])
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'bronze', dataset.bronze_table)} "
        f"({columns}) CLUSTER BY (_symbol, _period_start) "
        f"COMMENT 'Bronze {dataset.key}: raw records as strings, one file replaced atomically "
        "per load.'"
    )

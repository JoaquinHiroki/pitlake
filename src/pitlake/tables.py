from pitlake.bronze import sql_string
from pitlake.config import table_fqn
from pitlake.datasets import Dataset
from pitlake.manifest import SPARK_SCHEMA as MANIFEST_SPARK_SCHEMA
from pitlake.silver import SILVER_METADATA_COLUMNS, SilverSpec
from pitlake.venues import COMPARISON_VALUES

LANDING_MANIFEST = "landing_manifest"
BRONZE_LOAD_LOG = "bronze_load_log"
SOURCE_REGISTRY = "source_registry"
INGEST_COMMITS = "ingest_commits"

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


SOURCE_REGISTRY_SCHEMA = (
    "source STRING, dataset STRING, bronze_table STRING, archive STRING, file_format STRING, "
    "description STRING, registered_at TIMESTAMP"
)

INGEST_COMMITS_SCHEMA = (
    "run_id STRING, committed_at TIMESTAMP, source STRING, dataset STRING, bronze_table STRING, "
    "bronze_version BIGINT, files_loaded BIGINT, rows_loaded BIGINT"
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


def source_registry_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', SOURCE_REGISTRY)} "
        f"({SOURCE_REGISTRY_SCHEMA}) "
        "COMMENT 'Datasets the platform loads, rewritten from pitlake.datasets by every "
        "ingest run.'"
    )


def ingest_commits_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', INGEST_COMMITS)} "
        f"({INGEST_COMMITS_SCHEMA}) "
        "COMMENT 'One row per dataset per ingest run in which every source succeeded: the Bronze "
        "version downstream layers read. Append-only (ADR 0005).'"
    )


def raw_schema(dataset: Dataset) -> str:
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


QUARANTINE = "quarantine"
SEQUENCE_GAPS = "sequence_gaps"
REJECTION_RATES = "rejection_rates"
SILVER_LOAD_LOG = "silver_load_log"
QUALITY_RULES = "quality_rules"
QUALITY_HALTS = "quality_halts"
SILVER_COMMITS = "silver_commits"
CROSS_VENUE = "cross_venue_1m"
CROSS_VENUE_LOG = "cross_venue_log"
QUALITY_THRESHOLDS = "quality_thresholds"
QUALITY_WAIVERS = "quality_waivers"
QUALITY_GATE_RESULTS = "quality_gate_results"
SHARED_SILVER_TABLES = (QUARANTINE, SEQUENCE_GAPS, CROSS_VENUE)

_SILVER_METADATA = ", ".join(f"{name} {kind}" for name, kind in SILVER_METADATA_COLUMNS)

SILVER_LOAD_LOG_SCHEMA = (
    "silver_table STRING, source_file STRING, sha256 STRING, symbol STRING, period_start DATE, "
    "bronze_version BIGINT, rows_in BIGINT, rows_out BIGINT, rows_quarantined BIGINT, "
    "rejected_by_rule MAP<STRING, BIGINT>, sequence_gaps BIGINT, sequence_missing BIGINT, "
    "processed_at TIMESTAMP, run_id STRING"
)

QUALITY_RULES_SCHEMA = (
    "silver_table STRING, position INT, rule STRING, action STRING, `condition` STRING, "
    "description STRING, registered_at TIMESTAMP"
)

QUALITY_HALTS_SCHEMA = (
    "run_id STRING, halted_at TIMESTAMP, silver_table STRING, rule STRING, failed_rows BIGINT, "
    "sample STRING, injected_fault STRING"
)

SILVER_COMMITS_SCHEMA = (
    "run_id STRING, committed_at TIMESTAMP, silver_table STRING, silver_version BIGINT, "
    "files_processed BIGINT, rows_out BIGINT, rows_quarantined BIGINT"
)


def silver_ddl(catalog: str, spec: SilverSpec) -> str:
    columns = ", ".join([*(f"`{c.name}` {c.type}" for c in spec.output_columns), _SILVER_METADATA])
    cluster = ", ".join(spec.cluster_by)
    comment = sql_string(f"Silver {spec.key_name}: {spec.description}")
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'silver', spec.table)} ({columns}) "
        f"CLUSTER BY ({cluster}) COMMENT {comment}"
    )


# Partitioned by silver_table so parallel transform iterations replace disjoint partitions.
def quarantine_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'silver', QUARANTINE)} "
        "(silver_table STRING, rule STRING, record STRING, event_time TIMESTAMP, "
        f"knowable_at TIMESTAMP, {_SILVER_METADATA}) PARTITIONED BY (silver_table) "
        "COMMENT 'Rows a drop rule rejected, with the rule name and the Bronze record as received. "
        "Every Bronze row is in its Silver table or here.'"
    )


def sequence_gaps_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'silver', SEQUENCE_GAPS)} "
        "(silver_table STRING, rule STRING, sequence STRING, _symbol STRING, _period_start DATE, "
        "_source_file STRING, gap_start BIGINT, gap_end BIGINT, missing BIGINT, "
        "_recorded_at TIMESTAMP, _transform_run_id STRING) PARTITIONED BY (silver_table) "
        "COMMENT 'Gaps found by record-only sequence rules. The rows around a gap stay in Silver.'"
    )


def silver_load_log_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', SILVER_LOAD_LOG)} "
        f"({SILVER_LOAD_LOG_SCHEMA}) "
        "COMMENT 'Bronze files transformed into Silver, keyed by silver_table, source_file and "
        "sha256, with row counts per rule. Append-only.'"
    )


def quality_rules_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', QUALITY_RULES)} "
        f"({QUALITY_RULES_SCHEMA}) "
        "COMMENT 'Quality rules per Silver table in the order rows are judged, rewritten from "
        "pitlake.silver by every transform run.'"
    )


def quality_halts_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', QUALITY_HALTS)} "
        f"({QUALITY_HALTS_SCHEMA}) "
        "COMMENT 'Every time a halt rule stopped a transform run, with sample rows. Append-only.'"
    )


def silver_commits_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', SILVER_COMMITS)} "
        f"({SILVER_COMMITS_SCHEMA}) "
        "COMMENT 'One row per Silver table per transform run in which every table succeeded: the "
        "Silver version Gold reads. Append-only (ADR 0006).'"
    )


def rejection_rates_view(catalog: str) -> str:
    log = table_fqn(catalog, "control", SILVER_LOAD_LOG)
    rules = table_fqn(catalog, "control", QUALITY_RULES)
    quarantine = table_fqn(catalog, "silver", QUARANTINE)
    return f"""
        CREATE OR REPLACE VIEW {table_fqn(catalog, "silver", REJECTION_RATES)}
        COMMENT 'Rejection rate per Silver table and drop rule, with what each rule catches.'
        AS WITH judged AS (
          SELECT silver_table, sum(rows_in) AS rows_judged
          FROM (
            SELECT silver_table, rows_in FROM {log}
            QUALIFY row_number() OVER (
              PARTITION BY silver_table, source_file ORDER BY processed_at DESC) = 1)
          GROUP BY silver_table
        ), rejected AS (
          SELECT silver_table, rule, count(*) AS rows_rejected
          FROM {quarantine} GROUP BY silver_table, rule
        )
        SELECT r.silver_table, r.position, r.rule,
               coalesce(q.rows_rejected, 0) AS rows_rejected, j.rows_judged,
               coalesce(q.rows_rejected, 0) / nullif(j.rows_judged, 0) AS rejection_rate,
               r.description
        FROM {rules} AS r
        JOIN judged AS j ON j.silver_table = r.silver_table
        LEFT JOIN rejected AS q ON q.silver_table = r.silver_table AND q.rule = r.rule
        WHERE r.action = 'drop'
    """


CROSS_VENUE_LOG_SCHEMA = (
    "pair STRING, day DATE, left_file STRING, left_sha256 STRING, right_file STRING, "
    "right_sha256 STRING, minutes BIGINT, divergent BIGINT, missing_left BIGINT, "
    "missing_right BIGINT, compared_at TIMESTAMP, run_id STRING"
)

QUALITY_THRESHOLDS_SCHEMA = (
    "gate_check STRING, subject STRING, max_rate DOUBLE, tolerance_rows BIGINT, "
    "description STRING, registered_at TIMESTAMP"
)

QUALITY_WAIVERS_SCHEMA = (
    "gate_check STRING, subject STRING, day DATE, reason STRING, waived_by STRING, "
    "waived_at TIMESTAMP"
)

QUALITY_GATE_RESULTS_SCHEMA = (
    "run_id STRING, checked_at TIMESTAMP, gate_check STRING, subject STRING, day DATE, "
    "measured BIGINT, failed BIGINT, allowed DOUBLE, passed BOOLEAN, waived BOOLEAN, "
    "waiver_reason STRING, injected_fault STRING"
)


def cross_venue_ddl(catalog: str) -> str:
    values = ", ".join(f"{side}_{c} {kind.upper()}" for side, c, kind in COMPARISON_VALUES)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'silver', CROSS_VENUE)} "
        "(pair STRING, day DATE, event_time TIMESTAMP, knowable_at TIMESTAMP, status STRING, "
        f"close_diff_bps DOUBLE, {values}, _left_file STRING, _right_file STRING, "
        "_compared_at TIMESTAMP, _run_id STRING) CLUSTER BY (pair, event_time) "
        "COMMENT 'Every minute of every compared day on both crypto venues and whether they agree.'"
    )


def cross_venue_log_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', CROSS_VENUE_LOG)} "
        f"({CROSS_VENUE_LOG_SCHEMA}) "
        "COMMENT 'Days compared across venues, keyed by the Silver files compared. Append-only.'"
    )


def quality_thresholds_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', QUALITY_THRESHOLDS)} "
        f"({QUALITY_THRESHOLDS_SCHEMA}) "
        "COMMENT 'Quality gate limits, rewritten from pitlake.gate by every transform run.'"
    )


def quality_waivers_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', QUALITY_WAIVERS)} "
        f"({QUALITY_WAIVERS_SCHEMA}) "
        "COMMENT 'Gate breaches a person investigated and accepted, one row per check, subject and "
        "day, with the reason. Written by hand.'"
    )


def quality_gate_results_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', QUALITY_GATE_RESULTS)} "
        f"({QUALITY_GATE_RESULTS_SCHEMA}) "
        "COMMENT 'Every quality gate check of every transform run. Append-only.'"
    )

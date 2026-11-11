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


BARS_1M = "bars_1m"
BARS_1D = "bars_1d"
FUNDAMENTALS = "fundamentals"
FEATURES = "features"
GOLD_TABLES = (BARS_1M, BARS_1D, FUNDAMENTALS, FEATURES)
GOLD_BARS_LOG = "gold_bars_log"
GOLD_BUILDS = "gold_builds"
GOLD_COMMITS = "gold_commits"
LEAKAGE_CHECKS = "leakage_checks"
FILING_VISIBILITY = "filing_visibility"

_PRICES = "open DECIMAL(28,10), high DECIMAL(28,10), low DECIMAL(28,10), close DECIMAL(28,10)"
_GOLD_METADATA = "_built_at TIMESTAMP, _run_id STRING"

BARS_1M_SCHEMA = (
    f"venue STRING, symbol STRING, period DATE, bar_time TIMESTAMP, {_PRICES}, "
    "volume DECIMAL(38,10), trades BIGINT, vwap DECIMAL(28,10), knowable_at TIMESTAMP, "
    f"_source_file STRING, _source_sha256 STRING, _silver_version BIGINT, {_GOLD_METADATA}"
)

BARS_1D_SCHEMA = (
    f"venue STRING, symbol STRING, session_date DATE, {_PRICES}, volume DECIMAL(38,10), "
    "trades BIGINT, vwap DECIMAL(28,10), minutes INT, knowable_at TIMESTAMP, _source STRING, "
    f"{_GOLD_METADATA}"
)

FUNDAMENTALS_SCHEMA = (
    "symbol STRING, metric STRING, duration STRING, period_start DATE, period_end DATE, "
    "value DECIMAL(38,6), unit STRING, concept STRING, accession_number STRING, form STRING, "
    "filed_date DATE, knowable_at TIMESTAMP, knowable_basis STRING, _silver_version BIGINT, "
    f"{_GOLD_METADATA}"
)

FEATURES_SCHEMA = (
    "entity STRING NOT NULL, feature STRING NOT NULL, feature_group STRING, "
    "event_time TIMESTAMP NOT NULL, knowable_at TIMESTAMP NOT NULL, value DOUBLE, "
    f"source_table STRING, source_ref STRING NOT NULL, {_GOLD_METADATA}"
)

GOLD_BARS_LOG_SCHEMA = (
    "venue STRING, symbol STRING, period DATE, source_file STRING, sha256 STRING, "
    "silver_processed_at TIMESTAMP, silver_version BIGINT, bars BIGINT, built_at TIMESTAMP, "
    "run_id STRING"
)

GOLD_BUILDS_SCHEMA = (
    "run_id STRING, built_at TIMESTAMP, gold_table STRING, inputs MAP<STRING, BIGINT>, "
    "rows_written BIGINT"
)

GOLD_COMMITS_SCHEMA = (
    "run_id STRING, committed_at TIMESTAMP, gold_table STRING, gold_version BIGINT, "
    "inputs MAP<STRING, BIGINT>"
)

LEAKAGE_CHECKS_SCHEMA = (
    "run_id STRING, checked_at TIMESTAMP, leakage_check STRING, subject STRING, examined BIGINT, "
    "cases BIGINT, sample STRING, injected_fault STRING"
)

FILING_VISIBILITY_SCHEMA = (
    "run_id STRING, checked_at TIMESTAMP, symbol STRING, accession_number STRING, form STRING, "
    "filed_date DATE, period_end DATE, knowable_from TIMESTAMP, knowable_basis STRING, "
    "lead_days INT, features_before BIGINT, features_at BIGINT, injected_fault STRING"
)


GOLD_RETENTION = "interval 365 days"
RETENTION_PROPERTIES = ("delta.deletedFileRetentionDuration", "delta.logRetentionDuration")


def retention_properties() -> str:
    return ", ".join(f"'{p}' = '{GOLD_RETENTION}'" for p in RETENTION_PROPERTIES)


def _gold_ddl(
    catalog: str, table: str, schema: str, cluster: str, comment: str, properties: str = ""
) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'gold', table)} ({schema}) "
        f"CLUSTER BY ({cluster}) COMMENT {sql_string(comment)} "
        f"TBLPROPERTIES ({properties}{retention_properties()})"
    )


def gold_retention_ddl(catalog: str, table: str) -> str:
    return (
        f"ALTER TABLE {table_fqn(catalog, 'gold', table)} "
        f"SET TBLPROPERTIES ({retention_properties()})"
    )


def gold_ddls(catalog: str) -> list[str]:
    return [
        _gold_ddl(
            catalog,
            BARS_1M,
            BARS_1M_SCHEMA,
            "symbol, bar_time",
            "One-minute bars per venue: Binance and Alpaca trades compressed, Coinbase candles as "
            "published. Known when the minute ends (ADR 0008).",
        ),
        _gold_ddl(
            catalog,
            BARS_1D,
            BARS_1D_SCHEMA,
            "symbol, session_date",
            "Daily bars: crypto UTC days from bars_1m, equities from Alpaca consolidated bars. "
            "Known when the day ends.",
        ),
        _gold_ddl(
            catalog,
            FUNDAMENTALS,
            FUNDAMENTALS_SCHEMA,
            "symbol, metric",
            "Statement metrics once per filing that reported them, restatements included, known "
            "when that filing was accepted.",
        ),
        _gold_ddl(
            catalog,
            FEATURES,
            f"{FEATURES_SCHEMA}, CONSTRAINT gold_features_pk "
            "PRIMARY KEY (entity, feature, event_time, source_ref, knowable_at TIMESERIES)",
            "entity, feature, knowable_at",
            "Every feature value with the instant it became knowable. Read it only through "
            "pitlake.pit.as_of, which never returns a value knowable after the instant asked for.",
        ),
    ]


def gold_control_ddls(catalog: str) -> list[str]:
    tables = (
        (GOLD_BARS_LOG, GOLD_BARS_LOG_SCHEMA, "Silver files compressed into bars_1m. Append-only."),
        (GOLD_BUILDS, GOLD_BUILDS_SCHEMA, "Gold tables each run wrote, with the Silver versions."),
        (
            GOLD_COMMITS,
            GOLD_COMMITS_SCHEMA,
            "One row per Gold table per run whose leakage audit found no case: the Gold version "
            "research reads. Append-only (ADR 0008).",
        ),
        (LEAKAGE_CHECKS, LEAKAGE_CHECKS_SCHEMA, "Every leakage audit check of every run."),
        (
            FILING_VISIBILITY,
            FILING_VISIBILITY_SCHEMA,
            "Per filing and audit: features from it visible just before and at its acceptance.",
        ),
    )
    return [
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', name)} ({schema}) "
        f"COMMENT {sql_string(comment)}"
        for name, schema, comment in tables
    ]


BACKTEST_RUNS = "backtest_runs"
BACKTEST_RUNS_SCHEMA = (
    "backtest_id STRING, research_run_id STRING, created_at TIMESTAMP, strategy STRING, "
    "params STRING, period_start DATE, period_end DATE, table_versions MAP<STRING, BIGINT>, "
    "gold_commit_run_id STRING, silver_versions MAP<STRING, BIGINT>, code_commit STRING, "
    "package_version STRING, input_hash STRING, result_hash STRING, metrics MAP<STRING, DOUBLE>, "
    "mlflow_experiment STRING, mlflow_run_id STRING, replay_of STRING, reproduced BOOLEAN"
)


def backtest_runs_ddl(catalog: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', BACKTEST_RUNS)} "
        f"({BACKTEST_RUNS_SCHEMA}) "
        "COMMENT 'Every backtest and every replay: parameters, the table versions and code it ran "
        "on, hashes of its input and result, and its MLflow run. Append-only (ADR 0009).'"
    )


INFORMATION_SETS = "information_sets"
INFORMATION_ITEMS = "information_items"
INFORMATION_SETS_SCHEMA = (
    "enumeration_id STRING, run_id STRING, created_at TIMESTAMP, backtest_id STRING, "
    "research_run_id STRING, decision_at TIMESTAMP, period_start DATE, period_end DATE, "
    "table_versions MAP<STRING, BIGINT>, set_hash STRING, items BIGINT, "
    "items_by_kind MAP<STRING, BIGINT>, read_items BIGINT, unread_items BIGINT, "
    "later_items BIGINT, world_rows BIGINT, values_read BIGINT, omitted ARRAY<STRING>, "
    "unaccounted BIGINT, unaccounted_sample ARRAY<STRING>, recorded_input_hash STRING, "
    "rebuilt_input_hash STRING, reproduced BOOLEAN, complete BOOLEAN, code_commit STRING, "
    "package_version STRING"
)
ABLATIONS = "ablations"
ABLATION_CHANGES = "ablation_changes"
ABLATIONS_SCHEMA = (
    "ablation_id STRING, run_id STRING, created_at TIMESTAMP, research_run_id STRING, "
    "backtest_ids ARRAY<STRING>, decision_at TIMESTAMP, period_start DATE, period_end DATE, "
    "table_versions MAP<STRING, BIGINT>, ablation_mode STRING, withheld ARRAY<STRING>, "
    "set_hash STRING, ablated_set_hash STRING, rows_removed BIGINT, rows_recomputed BIGINT, "
    "world_rows BIGINT, rebuild_extra BIGINT, rebuild_missing BIGINT, "
    "rebuild_sample ARRAY<STRING>, owned_left BIGINT, unaccounted BIGINT, "
    "unaccounted_sample ARRAY<STRING>, recorded_input_hash STRING, ablated_input_hash STRING, "
    "inputs_changed BOOLEAN, coherent BOOLEAN, removal_seconds DOUBLE, inputs_seconds DOUBLE, "
    "rebuild_seconds DOUBLE, code_commit STRING, package_version STRING"
)
ABLATION_CHANGES_SCHEMA = (
    "ablation_id STRING, change STRING, entity STRING, feature STRING, event_time TIMESTAMP, "
    "knowable_at TIMESTAMP, value DOUBLE, source_table STRING, source_ref STRING"
)
DECISION_POINTS = "decision_points"
DECISION_SAMPLES = "decision_samples"
DECISION_POINTS_SCHEMA = (
    "point_id STRING, run_id STRING, created_at TIMESTAMP, decider STRING, config STRING, "
    "config_hash STRING, backtest_id STRING, research_run_id STRING, decision_at TIMESTAMP, "
    "table_versions MAP<STRING, BIGINT>, withheld ARRAY<STRING>, world_set_hash STRING, "
    "world_rows BIGINT, samples INT, rounds INT, decision STRING, round_decisions ARRAY<STRING>, "
    "agreement DOUBLE, part_agreement MAP<STRING, DOUBLE>, distinct_decisions INT, invalid INT, "
    "min_agreement DOUBLE, reproducible BOOLEAN, inputs_verified BOOLEAN, replay_of STRING, "
    "reproduced BOOLEAN, median_seconds DOUBLE, code_commit STRING, package_version STRING"
)
DECISION_SAMPLES_SCHEMA = (
    "point_id STRING, run_id STRING, created_at TIMESTAMP, decider STRING, round_index INT, "
    "sample_index INT, decision STRING, decision_hash STRING, input_hash STRING, "
    "tool_calls STRING, model_calls INT, seconds DOUBLE, error STRING, answer STRING"
)
ATTRIBUTION_POINTS = "attribution_points"
ATTRIBUTIONS = "attributions"
ATTRIBUTION_POINTS_SCHEMA = (
    "point_key STRING, run_id STRING, created_at TIMESTAMP, decider STRING, config STRING, "
    "config_hash STRING, backtest_id STRING, research_run_id STRING, decision_at TIMESTAMP, "
    "table_versions MAP<STRING, BIGINT>, set_hash STRING, items BIGINT, "
    "items_by_kind MAP<STRING, BIGINT>, samples INT, min_agreement DOUBLE, base_world_id STRING, "
    "base_decision STRING, settled STRING, support MAP<STRING, INT>, valid INT, "
    "calls ARRAY<STRING>, input_hash STRING, reproducible BOOLEAN, inputs_verified BOOLEAN, "
    "code_commit STRING, package_version STRING"
)
ATTRIBUTIONS_SCHEMA = (
    "world_id STRING, point_key STRING, run_id STRING, created_at TIMESTAMP, "
    "withheld ARRAY<STRING>, kind STRING, entity STRING, item_knowable_at TIMESTAMP, "
    "status STRING, input_hash STRING, inputs_changed BOOLEAN, samples INT, invalid INT, "
    "decision STRING, outcomes MAP<STRING, STRING>, effects MAP<STRING, DOUBLE>, "
    "p_values MAP<STRING, DOUBLE>, caused ARRAY<STRING>, valid BOOLEAN, rows_removed BIGINT, "
    "rows_recomputed BIGINT, rebuilt BOOLEAN, seconds DOUBLE, code_commit STRING, "
    "package_version STRING"
)
SUFFICIENCY_TESTS = "sufficiency_tests"
SUFFICIENT_SETS = "sufficient_sets"
SUFFICIENCY_TESTS_SCHEMA = (
    "test_id STRING, point_key STRING, run_id STRING, created_at TIMESTAMP, phase STRING, "
    "kept_hash STRING, kept_count BIGINT, dropped ARRAY<STRING>, status STRING, "
    "passed BOOLEAN, valid BOOLEAN, samples INT, invalid INT, stopped BOOLEAN, "
    "decision STRING, outcomes MAP<STRING, STRING>, p_values MAP<STRING, DOUBLE>, "
    "calls ARRAY<STRING>, input_hash STRING, rows_removed BIGINT, rows_recomputed BIGINT, "
    "rebuilt BOOLEAN, seconds DOUBLE, code_commit STRING, package_version STRING"
)
SUFFICIENT_SETS_SCHEMA = (
    "point_key STRING, run_id STRING, created_at TIMESTAMP, decider STRING, "
    "decision_at TIMESTAMP, candidates BIGINT, kept ARRAY<STRING>, "
    "kept_by_kind MAP<STRING, BIGINT>, kept_hash STRING, tests BIGINT, sampled BIGINT, "
    "confirm_id STRING, confirm_decision STRING, confirmed BOOLEAN, rebuilt BOOLEAN, "
    "ablation_id STRING, coherent BOOLEAN, code_commit STRING, package_version STRING"
)
INFORMATION_ITEMS_SCHEMA = (
    "enumeration_id STRING, item_id STRING, kind STRING, entity STRING, item_key STRING, "
    "knowable_at TIMESTAMP, digest STRING, world_rows BIGINT, status STRING, run_id STRING, "
    "created_at TIMESTAMP"
)


def counterfactual_ddls(catalog: str) -> list[str]:
    tables = (
        (
            INFORMATION_SETS,
            INFORMATION_SETS_SCHEMA,
            "One row per recorded backtest per enumeration: the items knowable at its last "
            "decision, the input hash rebuilt from them alone, and whether every item is "
            "accounted for. Append-only (ADR 0022).",
        ),
        (
            INFORMATION_ITEMS,
            INFORMATION_ITEMS_SCHEMA,
            "Every bar, filing and FRED release of an enumerated information set, with its "
            "content digest and whether the decision read it. Append-only (ADR 0022).",
        ),
        (
            ABLATIONS,
            ABLATIONS_SCHEMA,
            "One row per ablated world: the items withheld, the Gold rows removed and "
            "recomputed, the inputs it gives the backtest, whether it equals a full rebuild "
            "without those items, and what each step cost. Append-only (ADR 0023).",
        ),
        (
            ABLATION_CHANGES,
            ABLATION_CHANGES_SCHEMA,
            "The Gold feature rows each ablation removed or added; the base version with these "
            "changes is the ablated world. Append-only (ADR 0023).",
        ),
        (
            DECISION_POINTS,
            DECISION_POINTS_SCHEMA,
            "One row per decider, instant and world per harness run: its configuration hash, "
            "the modal decision of every round and of all samples, how many samples agree, and "
            "whether it reproduced. Append-only (ADR 0024).",
        ),
        (
            DECISION_SAMPLES,
            DECISION_SAMPLES_SCHEMA,
            "Every sampled decision: the decision, the hash of the inputs its decider read, its "
            "tool calls, model calls and seconds; for attribution, point_id is the world_id. "
            "Append-only (ADR 0024, ADR 0025).",
        ),
        (
            ATTRIBUTION_POINTS,
            ATTRIBUTION_POINTS_SCHEMA,
            "One row per attributed decision: the decider, instant and Gold versions, its decision "
            "in the real world, the parts that settled and how many samples support each, and the "
            "tool calls it made. Append-only (ADR 0025).",
        ),
        (
            ATTRIBUTIONS,
            ATTRIBUTIONS_SCHEMA,
            "One row per decision and world with one item withheld, or none for the null world: "
            "whether the decider's inputs changed, and per settled part the outcome, the effect "
            "and its p-value, and whether the world equals a full rebuild. Append-only "
            "(ADR 0025).",
        ),
        (
            SUFFICIENCY_TESTS,
            SUFFICIENCY_TESTS_SCHEMA,
            "One row per decision and world tried in the search for a minimal sufficient set: "
            "the items kept, the chunk dropped, whether the decision stayed the same, how it was "
            "decided (evaluated, screened or sampled) and what it cost. Append-only (ADR 0026).",
        ),
        (
            SUFFICIENT_SETS,
            SUFFICIENT_SETS_SCHEMA,
            "One row per decision with a minimal sufficient set: the items in it, how many worlds "
            "the search tried, and whether the world built from the set alone decides the same, "
            "equals a full rebuild and is a coherent ablation. Append-only (ADR 0026).",
        ),
    )
    return [
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', name)} ({schema}) "
        f"COMMENT {sql_string(comment)}"
        for name, schema, comment in tables
    ]


FILING_CHUNKS = "filing_chunks"
CHUNK_EMBEDDINGS = "chunk_embeddings"
RAG_QUESTIONS = "rag_questions"
RAG_TABLES = (FILING_CHUNKS, CHUNK_EMBEDDINGS, RAG_QUESTIONS)
RAG_BUILDS = "rag_builds"
RETRIEVAL_RUNS = "retrieval_runs"
RAG_ANSWERS = "rag_answers"

_FILING = (
    "chunk_id STRING NOT NULL, symbol STRING, accession_number STRING, form STRING, "
    "filing_date DATE, report_date DATE, section STRING, chunk_index INT, context STRING, "
    "text STRING, knowable_at TIMESTAMP NOT NULL"
)
FILING_CHUNKS_SCHEMA = f"{_FILING}, _silver_version BIGINT, {_GOLD_METADATA}"
CHUNK_EMBEDDINGS_SCHEMA = (
    f"{_FILING}, knowable_at_us BIGINT NOT NULL, embedding ARRAY<FLOAT> NOT NULL, "
    f"embedding_model STRING NOT NULL, {_GOLD_METADATA}"
)
RAG_QUESTIONS_SCHEMA = (
    "question_id STRING NOT NULL, split STRING, symbol STRING, metric STRING, duration STRING, "
    "period_end DATE, value DECIMAL(38,6), accession_number STRING, as_of TIMESTAMP, "
    "as_of_us BIGINT, text STRING, answers ARRAY<STRING>, relevant ARRAY<STRING>, "
    f"embedding ARRAY<FLOAT>, embedding_model STRING, {_GOLD_METADATA}"
)
RAG_BUILDS_SCHEMA = (
    "run_id STRING, built_at TIMESTAMP, step STRING, rag_table STRING, rag_version BIGINT, "
    "rows_total BIGINT, rows_changed BIGINT, inputs MAP<STRING, BIGINT>, detail STRING"
)
RETRIEVAL_RUNS_SCHEMA = (
    "retrieval_run_id STRING, created_at TIMESTAMP, retriever STRING, split STRING, "
    "point_in_time BOOLEAN, metrics MAP<STRING, DOUBLE>, rankings_hash STRING, "
    "adapter_sha256 STRING, train_config STRING, table_versions MAP<STRING, BIGINT>, "
    "embedding_model STRING, code_commit STRING, package_version STRING, "
    "mlflow_experiment STRING, mlflow_run_id STRING, replay_of STRING, reproduced BOOLEAN"
)
RAG_ANSWERS_SCHEMA = (
    "answer_id STRING, asked_at TIMESTAMP, question STRING, as_of TIMESTAMP, symbol STRING, "
    "retrieval_run_id STRING, adapter_sha256 STRING, vector_index STRING, "
    "retrieved ARRAY<STRUCT<chunk_id: STRING, accession_number: STRING, form: STRING, "
    "knowable_at_us: BIGINT, score: DOUBLE>>, answer STRING, "
    "embedding_model STRING, chat_model STRING, mlflow_run_id STRING"
)


def rag_ddls(catalog: str) -> list[str]:
    gold = [
        _gold_ddl(
            catalog,
            FILING_CHUNKS,
            FILING_CHUNKS_SCHEMA,
            "symbol, knowable_at",
            "Passages of every 10-K and 10-Q, each known when its filing was accepted (ADR 0010).",
        ),
        _gold_ddl(
            catalog,
            CHUNK_EMBEDDINGS,
            CHUNK_EMBEDDINGS_SCHEMA,
            "symbol, knowable_at",
            "Unit-length embeddings of filing_chunks, the source of the vector index. Search it "
            "only with knowable_at_us <= the instant asked about (ADR 0010).",
            "'delta.enableChangeDataFeed' = 'true', ",
        ),
        _gold_ddl(
            catalog,
            RAG_QUESTIONS,
            RAG_QUESTIONS_SCHEMA,
            "split, symbol",
            "Questions generated from Gold fundamentals, labelled with the chunks of the filing "
            "that first reported the answer, and their embeddings (ADR 0010).",
        ),
    ]
    control = (
        (RAG_BUILDS, RAG_BUILDS_SCHEMA, "Every chunk, embed and index step, with versions."),
        (
            RETRIEVAL_RUNS,
            RETRIEVAL_RUNS_SCHEMA,
            "Every retrieval evaluation and replay: metrics per retriever and split, the table "
            "versions and adapter it used. Append-only (ADR 0010).",
        ),
        (RAG_ANSWERS, RAG_ANSWERS_SCHEMA, "Every answered question with what it retrieved."),
    )
    return gold + [
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', name)} ({schema}) "
        f"COMMENT {sql_string(comment)}"
        for name, schema, comment in control
    ]


JOB_COSTS = "job_costs"
JOB_COSTS_SCHEMA = (
    "usage_date DATE, workload STRING, job_id STRING, product STRING, sku_name STRING, "
    "unit STRING, quantity DOUBLE, list_usd DOUBLE, unpriced BIGINT, recorded_at TIMESTAMP, "
    "run_id STRING"
)
TABLE_HEALTH = "table_health"
TABLE_HEALTH_SCHEMA = (
    "checked_at TIMESTAMP, run_id STRING, layer STRING, table_name STRING, files BIGINT, "
    "bytes BIGINT, clustering ARRAY<STRING>, file_retention STRING, log_retention STRING, "
    "last_maintained MAP<STRING, TIMESTAMP>, failed_maintenance BIGINT, issues ARRAY<STRING>"
)

FRESHNESS = "freshness"
FRESHNESS_SCHEMA = (
    "checked_at TIMESTAMP, run_id STRING, silver_table STRING, newest_knowable_at TIMESTAMP, "
    "last_commit TIMESTAMP, age_days DOUBLE, stale_after_days INT, stale BOOLEAN"
)

DRIFT = "drift"
DRIFT_SCHEMA = (
    "checked_at TIMESTAMP, run_id STRING, silver_table STRING, slice STRING, column_name STRING, "
    "drift_check STRING, window_start TIMESTAMP, baseline_start TIMESTAMP, factor DOUBLE, "
    "allowed_factor DOUBLE, appeared ARRAY<STRING>, disappeared ARRAY<STRING>, drifted BOOLEAN"
)

QUERY_STATS = "query_stats"
QUERY_STATS_SCHEMA = (
    "day DATE, workload STRING, job_id STRING, queries BIGINT, duration_s DOUBLE, "
    "read_files BIGINT, pruned_files BIGINT, read_bytes BIGINT, read_rows BIGINT, "
    "recorded_at TIMESTAMP, run_id STRING"
)


def operations_ddls(catalog: str) -> list[str]:
    tables = (
        (
            JOB_COSTS,
            JOB_COSTS_SCHEMA,
            "Daily usage and list-price cost per job and per platform product, from the "
            "workspace's system tables. The last 30 days are rewritten on every run (ADR 0011).",
        ),
        (
            TABLE_HEALTH,
            TABLE_HEALTH_SCHEMA,
            "Size, files, retention and the last predictive optimization of every table, one row "
            "per table per operations run. Append-only (ADR 0011).",
        ),
        (
            FRESHNESS,
            FRESHNESS_SCHEMA,
            "How old the newest knowable row of every Silver table is, against the tolerance of "
            "its dataset, one row per table per operations run. Append-only (ADR 0011).",
        ),
        (
            DRIFT,
            DRIFT_SCHEMA,
            "Each monitored column's newest full window against the full window before it: "
            "changed categories, or a median that moved by more than its factor. One row per "
            "column per slice per operations run. Append-only (ADR 0011).",
        ),
        (
            QUERY_STATS,
            QUERY_STATS_SCHEMA,
            "Daily queries, time, files read and skipped, bytes and rows per job or client, from "
            "the workspace's query history. The last 30 days are rewritten on every run "
            "(ADR 0013).",
        ),
    )
    return [
        f"CREATE TABLE IF NOT EXISTS {table_fqn(catalog, 'control', name)} ({schema}) "
        f"COMMENT {sql_string(comment)}"
        for name, schema, comment in tables
    ]

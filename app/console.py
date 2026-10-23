import re
from collections.abc import Iterable
from datetime import date

READS = (
    "control.source_registry",
    "control.silver_load_log",
    "control.freshness",
    "control.quality_rules",
    "control.cross_venue_log",
    "control.quality_gate_results",
    "control.backtest_runs",
)
NO_FAULT = "none"
HEADLINE_METRICS = ("sharpe", "total_return", "annual_volatility", "max_drawdown")


def validate_catalog(catalog: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", catalog or ""):
        raise ValueError(f"invalid catalog name: {catalog!r}")
    return catalog


def table(catalog: str, name: str) -> str:
    if name not in READS:
        raise ValueError(f"{name} is not among the tables the console may read")
    schema, table_name = name.split(".")
    return f"`{validate_catalog(catalog)}`.`{schema}`.`{table_name}`"


def _latest_files(catalog: str) -> str:
    return (
        f"SELECT * FROM {table(catalog, 'control.silver_load_log')} QUALIFY row_number() OVER "
        "(PARTITION BY silver_table, source_file ORDER BY processed_at DESC) = 1"
    )


def coverage_query(catalog: str) -> str:
    return (
        "SELECT silver_table, symbol, min(period_start) AS first_period, "
        "max(period_start) AS last_period, count(DISTINCT period_start) AS periods, "
        "sum(rows_out) AS rows_out, sum(rows_quarantined) AS rows_quarantined, "
        f"max(processed_at) AS last_processed FROM ({_latest_files(catalog)}) "
        "GROUP BY ALL ORDER BY silver_table, symbol"
    )


def daily_rows_query(catalog: str) -> str:
    return (
        "SELECT period_start, symbol, sum(rows_out) AS rows_out, "
        f"sum(rows_quarantined) AS rows_quarantined FROM ({_latest_files(catalog)}) "
        "WHERE silver_table = :silver_table AND period_start >= :since "
        "GROUP BY ALL ORDER BY period_start, symbol"
    )


def freshness_query(catalog: str) -> str:
    source = table(catalog, "control.freshness")
    return (
        "SELECT silver_table, newest_knowable_at, last_commit, round(age_days, 1) AS age_days, "
        f"stale_after_days, stale, checked_at FROM {source} "
        f"WHERE checked_at = (SELECT max(checked_at) FROM {source}) ORDER BY silver_table"
    )


def rejection_query(catalog: str) -> str:
    return (
        f"WITH files AS ({_latest_files(catalog)}), "
        "judged AS (SELECT silver_table, sum(rows_in) AS rows_in FROM files "
        "WHERE period_start >= :since GROUP BY 1), "
        "rejected AS (SELECT silver_table, rule, sum(CAST(n AS BIGINT)) AS rejected FROM files "
        "LATERAL VIEW explode(rejected_by_rule) AS rule, n WHERE period_start >= :since "
        "GROUP BY ALL) "
        "SELECT r.silver_table, r.rule, r.description, j.rows_in, "
        "coalesce(x.rejected, 0) AS rejected, coalesce(x.rejected, 0) / j.rows_in AS rate "
        f"FROM {table(catalog, 'control.quality_rules')} r "
        "JOIN judged j ON j.silver_table = r.silver_table AND j.rows_in > 0 "
        "LEFT JOIN rejected x ON x.silver_table = r.silver_table AND x.rule = r.rule "
        "WHERE r.action = 'drop' ORDER BY rate DESC, r.silver_table, r.position"
    )


def cross_venue_query(catalog: str) -> str:
    return (
        "SELECT pair, day, minutes, divergent, missing_left, missing_right, compared_at "
        f"FROM {table(catalog, 'control.cross_venue_log')} WHERE day >= :since "
        "QUALIFY row_number() OVER (PARTITION BY pair, day ORDER BY compared_at DESC) = 1 "
        "ORDER BY day DESC, pair"
    )


def gate_query(catalog: str) -> str:
    source = table(catalog, "control.quality_gate_results")
    return (
        "SELECT run_id, checked_at, gate_check, subject, day, measured, failed, allowed, passed, "
        f"waived, waiver_reason FROM {source} WHERE run_id = (SELECT max_by(run_id, checked_at) "
        f"FROM {source} WHERE injected_fault = '{NO_FAULT}') "
        "ORDER BY passed, waived DESC, gate_check, subject, day"
    )


def backtests_query(catalog: str) -> str:
    return (
        "SELECT backtest_id, created_at, strategy, params, period_start, period_end, metrics, "
        "replay_of, reproduced, code_commit, package_version, gold_commit_run_id, "
        "table_versions, silver_versions, input_hash, result_hash, mlflow_experiment, "
        f"mlflow_run_id FROM {table(catalog, 'control.backtest_runs')} "
        "ORDER BY created_at DESC LIMIT 500"
    )


def registry_query(catalog: str) -> str:
    return (
        "SELECT concat(source, '.', dataset) AS dataset, description "
        f"FROM {table(catalog, 'control.source_registry')} ORDER BY source, dataset"
    )


def as_dict(value) -> dict:
    if value is None:
        return {}
    return dict(value.items() if isinstance(value, dict) else value)


def headline(metrics) -> dict[str, float | None]:
    found = as_dict(metrics)
    return {m: None if found.get(m) is None else float(found[m]) for m in HEADLINE_METRICS}


def ingest_parameters(
    datasets: Iterable[str],
    start: date | None,
    end: date | None,
    reload: bool,
    max_files: int,
    registered: Iterable[str],
) -> dict[str, str]:
    datasets = list(datasets)
    unknown = sorted(set(datasets) - set(registered))
    if not datasets:
        raise ValueError("Choose at least one dataset.")
    if unknown:
        raise ValueError(f"Not registered: {unknown}.")
    if start and end and start > end:
        raise ValueError("The first day is after the last day.")
    if reload and (start is None or end is None):
        raise ValueError("A reload needs both a first and a last day.")
    if max_files < 0:
        raise ValueError("The file limit cannot be negative; 0 means no limit.")
    return {
        "datasets": ",".join(datasets),
        "period_start": start.isoformat() if start else "",
        "period_end": end.isoformat() if end else "",
        "reload": "true" if reload else "false",
        "max_files": str(max_files),
    }

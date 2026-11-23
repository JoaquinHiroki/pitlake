import json
import math
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
    "control.query_stats",
    "results.attribution_points",
    "results.attributions",
    "results.sufficient_sets",
    "results.horizon_curves",
    "results.monotonicity_pairs",
    "results.decompositions",
    "results.source_values",
)
NO_FAULT = "none"
ALPHA = 0.05
SAME = "same"
MONOTONIC = "MONOTONIC"
NON_MONOTONIC = "NON-MONOTONIC"
HEADLINE_METRICS = ("sharpe", "total_return", "annual_volatility", "max_drawdown")
INTEGERS = {"BYTE", "SHORT", "INT", "LONG"}
DECIMALS = {"FLOAT", "DOUBLE", "DECIMAL"}
NESTED = {"ARRAY", "MAP", "STRUCT"}


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
        f"WITH files AS ({_latest_files(catalog)} AND silver_table = :silver_table) "
        "SELECT period_start, symbol, sum(rows_out) AS rows_out, "
        "sum(rows_quarantined) AS rows_quarantined FROM files "
        "WHERE period_start > date_sub((SELECT max(period_start) FROM files), :days) "
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
        f"WITH latest AS ({_latest_files(catalog)}), "
        "files AS (SELECT * FROM latest QUALIFY period_start > "
        "date_sub(max(period_start) OVER (PARTITION BY silver_table), :days)), "
        "judged AS (SELECT silver_table, sum(rows_in) AS rows_in, "
        "min(period_start) AS first_period, max(period_start) AS last_period "
        "FROM files GROUP BY 1), "
        "rejected AS (SELECT silver_table, rule, sum(CAST(n AS BIGINT)) AS rejected FROM files "
        "LATERAL VIEW explode(rejected_by_rule) AS rule, n GROUP BY ALL) "
        "SELECT r.silver_table, r.rule, r.description, j.first_period, j.last_period, j.rows_in, "
        "coalesce(x.rejected, 0) AS rejected, coalesce(x.rejected, 0) / j.rows_in AS rate "
        f"FROM {table(catalog, 'control.quality_rules')} r "
        "JOIN judged j ON j.silver_table = r.silver_table AND j.rows_in > 0 "
        "LEFT JOIN rejected x ON x.silver_table = r.silver_table AND x.rule = r.rule "
        "WHERE r.action = 'drop' ORDER BY rate DESC, r.silver_table, r.position"
    )


def cross_venue_query(catalog: str) -> str:
    source = table(catalog, "control.cross_venue_log")
    return (
        "SELECT pair, day, minutes, divergent, missing_left, missing_right, compared_at "
        f"FROM {source} WHERE day > date_sub((SELECT max(day) FROM {source}), :days) "
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
    headline = ", ".join(f"try_cast(metrics['{m}'] AS DOUBLE) AS {m}" for m in HEADLINE_METRICS)
    return (
        "SELECT backtest_id, created_at, strategy, period_start, period_end, "
        f"{headline}, replay_of, reproduced, params, metrics, "
        "code_commit, package_version, gold_commit_run_id, "
        "table_versions, silver_versions, input_hash, result_hash, mlflow_experiment, "
        f"mlflow_run_id FROM {table(catalog, 'control.backtest_runs')} "
        "ORDER BY created_at DESC LIMIT 500"
    )


def performance_query(catalog: str) -> str:
    source = table(catalog, "control.query_stats")
    return (
        "SELECT day, workload, queries, duration_s, read_files, pruned_files, "
        "pruned_files / nullif(read_files + pruned_files, 0) AS pruned_share, read_bytes, "
        f"read_rows FROM {source} "
        f"WHERE day > date_sub((SELECT max(day) FROM {source}), :days) "
        "AND workload LIKE 'pitlake-%' ORDER BY day, workload"
    )


def registry_query(catalog: str) -> str:
    return (
        "SELECT concat(source, '.', dataset) AS dataset, description "
        f"FROM {table(catalog, 'control.source_registry')} ORDER BY source, dataset"
    )


def _points(catalog: str) -> str:
    return (
        f"SELECT * FROM {table(catalog, 'results.attribution_points')} "
        "QUALIFY row_number() OVER (PARTITION BY point_key ORDER BY created_at DESC) = 1"
    )


def _worlds(catalog: str) -> str:
    return (
        f"SELECT * FROM {table(catalog, 'results.attributions')} WHERE valid "
        "QUALIFY row_number() OVER "
        "(PARTITION BY point_key, array_join(withheld, ' ') ORDER BY created_at DESC) = 1"
    )


def _null_passed(world: str) -> str:
    return (
        f"{world}.valid AND size({world}.caused) = 0 "
        f"AND NOT exists(map_values({world}.outcomes), o -> o != '{SAME}') "
        f"AND NOT exists(map_values({world}.p_values), p -> p < {ALPHA})"
    )


def attribution_query(catalog: str) -> str:
    return (
        f"WITH points AS ({_points(catalog)}), worlds AS ({_worlds(catalog)}) "
        "SELECT p.point_key, p.decider, p.decision_at, p.base_decision, p.settled, p.items, "
        "count_if(size(w.withheld) > 0) AS worlds, "
        "count_if(size(w.withheld) > 0 AND w.status = 'screened') AS screened, "
        "count_if(size(w.withheld) > 0 AND size(w.caused) > 0) AS causing, "
        f"bool_or({_null_passed('w')}) FILTER (WHERE size(w.withheld) = 0) "
        "AS null_changes_nothing, "
        "count_if(w.rebuilt) AS equal_to_rebuild, count_if(NOT w.rebuilt) AS differ_from_rebuild, "
        "p.reproducible, p.backtest_id, p.created_at "
        "FROM points p LEFT JOIN worlds w ON w.point_key = p.point_key "
        "GROUP BY ALL ORDER BY p.backtest_id, p.decision_at, p.decider"
    )


def causes_query(catalog: str) -> str:
    return (
        f"WITH worlds AS ({_worlds(catalog)}) "
        "SELECT array_join(withheld, ' ') AS item, kind, entity, item_knowable_at, caused, "
        "array_max(transform(caused, c -> abs(effects[c]))) AS largest_effect, outcomes, "
        "effects, p_values, status FROM worlds "
        "WHERE point_key = :point_key AND size(withheld) > 0 AND size(caused) > 0 "
        "ORDER BY largest_effect DESC, item"
    )


def sufficiency_query(catalog: str) -> str:
    return (
        f"WITH points AS ({_points(catalog)}), sets AS (SELECT * FROM "
        f"{table(catalog, 'results.sufficient_sets')} "
        "QUALIFY row_number() OVER (PARTITION BY point_key ORDER BY created_at DESC) = 1) "
        "SELECT s.point_key, s.decider, s.decision_at, p.base_decision, s.candidates, "
        "size(s.kept) AS kept_count, s.kept_by_kind, s.tests, s.sampled, s.confirmed, "
        "s.confirm_decision, s.rebuilt, s.coherent, s.ablation_id, p.backtest_id, s.created_at, "
        "s.kept FROM sets s LEFT JOIN points p ON p.point_key = s.point_key "
        "ORDER BY p.backtest_id, s.decision_at, s.decider"
    )


def _latest(catalog: str, name: str, *by: str) -> str:
    return (
        f"SELECT * FROM {table(catalog, name)} QUALIFY dense_rank() OVER "
        f"(PARTITION BY {', '.join(by)} ORDER BY created_at DESC) = 1"
    )


def horizon_query(catalog: str) -> str:
    return (
        f"WITH curves AS ({_latest(catalog, 'results.horizon_curves', 'point_key')}) "
        "SELECT point_key, decider, decision_at, any_value(holds_to) AS holds_to, "
        "any_value(breaks_at) AS breaks_at, count(*) AS steps, "
        "count_if(boundary IS NOT NULL) AS boundary_ablations, "
        "count_if(boundary IS NOT NULL AND coherent) AS coherent_ablations, "
        "any_value(ladder) AS ladder, max(created_at) AS created_at FROM curves "
        "GROUP BY point_key, decider, decision_at ORDER BY decision_at, decider"
    )


def horizon_steps_query(catalog: str) -> str:
    return (
        f"WITH curves AS ({_latest(catalog, 'results.horizon_curves', 'point_key')}) "
        "SELECT horizon_days, phase, cutoff, kept_count, reproduced, same, agreement, outcomes, "
        "rebuilt, boundary, ablation_id, coherent FROM curves WHERE point_key = :point_key "
        "ORDER BY horizon_days DESC"
    )


def monotonicity_query(catalog: str) -> str:
    return (
        f"WITH pairs AS ({_latest(catalog, 'results.monotonicity_pairs', 'decider')}) "
        f"SELECT decider, CASE WHEN count_if(falls) = 0 THEN '{MONOTONIC}' "
        f"ELSE '{NON_MONOTONIC}' END AS verdict, any_value(decisions) AS decisions, "
        "any_value(chains) AS chains, any_value(outcome_days) AS outcome_days, count(*) AS pairs, "
        "count_if(falls) AS pairs_falling, count_if(rises) AS pairs_rising, "
        "max(created_at) AS created_at FROM pairs GROUP BY decider ORDER BY decider"
    )


def quality_query(catalog: str) -> str:
    return (
        f"WITH pairs AS ({_latest(catalog, 'results.monotonicity_pairs', 'decider')}) "
        "SELECT decider, low AS from_share, high AS share, quality_low, quality_high, change, "
        "p_falls, holm_falls, p_rises, holm_rises, falls, rises FROM pairs "
        "QUALIFY low = min(low) OVER (PARTITION BY decider) ORDER BY decider, high"
    )


def decomposition_query(catalog: str) -> str:
    latest = _latest(catalog, "results.decompositions", "endpoint", "backtest_id", "cutoff")
    return (
        f"WITH found AS ({latest}) "
        "SELECT endpoint, cutoff, component, era, instants, estimate, low, high, level, "
        "outcome_days, samples, backtest_id, created_at FROM found "
        "ORDER BY endpoint, cutoff, component, era"
    )


def sources_query(catalog: str) -> str:
    latest = _latest(catalog, "results.source_values", "backtest_id", "decider")
    return (
        f"WITH found AS ({latest}) "
        "SELECT backtest_id, decider, source, level, verdict, decisions, decision_days, changed, "
        "parts, parts_changed, largest_effect, knowable, attributed, inputs_changed, "
        "items_caused, parts_caused, created_at FROM found "
        "ORDER BY backtest_id, decider, level, source"
    )


VIEWS = {
    "coverage": (coverage_query, {}),
    "daily_rows": (daily_rows_query, {"silver_table": "STRING", "days": "INT"}),
    "freshness": (freshness_query, {}),
    "rejections": (rejection_query, {"days": "INT"}),
    "cross_venue": (cross_venue_query, {"days": "INT"}),
    "gate": (gate_query, {}),
    "backtests": (backtests_query, {}),
    "performance": (performance_query, {"days": "INT"}),
    "registry": (registry_query, {}),
    "attribution": (attribution_query, {}),
    "causes": (causes_query, {"point_key": "STRING"}),
    "sufficiency": (sufficiency_query, {}),
    "horizon": (horizon_query, {}),
    "horizon_steps": (horizon_steps_query, {"point_key": "STRING"}),
    "monotonicity": (monotonicity_query, {}),
    "quality": (quality_query, {}),
    "decomposition": (decomposition_query, {}),
    "sources": (sources_query, {}),
}


def typed(value: str | None, type_name: str | None):
    if value is None:
        return None
    if type_name in INTEGERS:
        return int(value)
    if type_name in DECIMALS:
        number = float(value)
        return number if math.isfinite(number) else None
    if type_name == "BOOLEAN":
        return value == "true"
    if type_name in NESTED:
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def result_table(statement: dict) -> dict:
    manifest = statement.get("manifest", {})
    columns = [
        {"name": c["name"], "type": c.get("type_name")}
        for c in manifest.get("schema", {}).get("columns", [])
    ]
    rows = [
        [typed(v, c["type"]) for v, c in zip(row, columns, strict=True)]
        for row in statement.get("result", {}).get("data_array", []) or []
    ]
    return {
        "columns": columns,
        "rows": rows,
        "row_count": manifest.get("total_row_count", len(rows)),
        "truncated": bool(manifest.get("truncated")),
    }


def records(table: dict) -> list[dict]:
    names = [c["name"] for c in table["columns"]]
    return [dict(zip(names, row, strict=True)) for row in table["rows"]]


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

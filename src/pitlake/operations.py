import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from pitlake.backtest import as_utc
from pitlake.config import table_fqn, validate_identifier
from pitlake.datasets import Dataset
from pitlake.tables import GOLD_RETENTION, RETENTION_PROPERTIES, SILVER_COMMITS

LAYERS = ("bronze", "silver", "gold", "control")
STAGING_LAYERS = ("bronze", "silver", "gold")
NESTED_STAGING = frozenset({"generator"})
COST_LOOKBACK_DAYS = 30
STAGING_DAYS = 7
SKIPPED_PREFIXES = ("__", "event_log_")


def costs_query(since: date) -> tuple[str, dict]:
    return (
        "SELECT u.usage_date, "
        "coalesce(j.name, lower(u.billing_origin_product)) AS workload, "
        "u.usage_metadata.job_id AS job_id, u.billing_origin_product AS product, u.sku_name, "
        "u.usage_unit AS unit, sum(u.usage_quantity) AS quantity, "
        "sum(u.usage_quantity * p.pricing.default) AS list_usd, "
        "count(*) - count(p.sku_name) AS unpriced "
        "FROM system.billing.usage u "
        "LEFT JOIN system.billing.list_prices p ON p.sku_name = u.sku_name "
        "AND p.usage_unit = u.usage_unit AND p.currency_code = 'USD' "
        "AND u.usage_end_time >= p.price_start_time "
        "AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time) "
        "LEFT JOIN (SELECT job_id, max_by(name, change_time) AS name FROM system.lakeflow.jobs "
        "GROUP BY job_id) j ON u.usage_metadata.job_id = j.job_id "
        "WHERE u.usage_date >= :since "
        "GROUP BY ALL",
        {"since": since},
    )


def maintenance_query(catalog: str) -> tuple[str, dict]:
    return (
        "SELECT schema_name, table_name, operation_type, max(end_time) AS last_run, "
        "count(*) AS runs, sum(CASE WHEN operation_status = 'SUCCESSFUL' THEN 0 ELSE 1 END) "
        "AS failed "
        "FROM system.storage.predictive_optimization_operations_history "
        "WHERE catalog_name = :catalog GROUP BY ALL",
        {"catalog": validate_identifier(catalog)},
    )


def tables_query(catalog: str) -> str:
    layers = ", ".join(f"'{layer}'" for layer in LAYERS)
    return (
        "SELECT table_schema, table_name "
        f"FROM {validate_identifier(catalog)}.information_schema.tables "
        f"WHERE table_type = 'MANAGED' AND table_schema IN ({layers}) "
        "ORDER BY table_schema, table_name"
    )


def maintained(table: str) -> bool:
    return not table.startswith(SKIPPED_PREFIXES)


def retention_issues(schema: str, properties: Mapping[str, str]) -> list[str]:
    if schema != "gold":
        return []
    wanted = GOLD_RETENTION.removeprefix("interval ")
    return [
        f"{p} is {properties.get(p, 'the default')}, not {wanted}"
        for p in RETENTION_PROPERTIES
        if properties.get(p, "").removeprefix("interval ") != wanted
    ]


@dataclass(frozen=True)
class Staged:
    path: Path
    newest: float


def staged_folders(root: Path) -> Iterable[Staged]:
    if not root.is_dir():
        return
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        children = [entry] if entry.name not in NESTED_STAGING else sorted(entry.iterdir())
        for folder in children:
            if folder.is_dir():
                yield Staged(folder, newest_file(folder))


def newest_file(folder: Path) -> float:
    newest = 0.0
    for parent, _, files in os.walk(folder):
        for name in files:
            newest = max(newest, (Path(parent) / name).stat().st_mtime)
    return newest


def expired(staged: Iterable[Staged], now: float, days: int = STAGING_DAYS) -> list[Path]:
    cutoff = now - days * 86_400
    return [s.path for s in staged if s.newest < cutoff]


def freshness_query(catalog: str, datasets: Iterable[Dataset]) -> str:
    newest = " UNION ALL ".join(
        f"SELECT '{d.bronze_table}' AS silver_table, max(knowable_at) AS newest "
        f"FROM {table_fqn(catalog, 'silver', d.bronze_table)}"
        for d in datasets
    )
    commits = table_fqn(catalog, "control", SILVER_COMMITS)
    return (
        f"SELECT n.silver_table, n.newest, c.committed FROM ({newest}) n LEFT JOIN "
        f"(SELECT silver_table, max(committed_at) AS committed FROM {commits} GROUP BY 1) c "
        "ON n.silver_table = c.silver_table"
    )


@dataclass(frozen=True)
class Freshness:
    silver_table: str
    newest: datetime | None
    committed: datetime | None
    age_days: float | None
    stale_after_days: int

    @property
    def stale(self) -> bool:
        return self.age_days is None or self.age_days > self.stale_after_days


def judge_freshness(
    found: Mapping[str, tuple[datetime | None, datetime | None]],
    datasets: Iterable[Dataset],
    now: datetime,
) -> list[Freshness]:
    judged = []
    for d in datasets:
        newest, committed = (
            None if value is None else as_utc(value)
            for value in found.get(d.bronze_table, (None, None))
        )
        age = None if newest is None else (as_utc(now) - newest).total_seconds() / 86_400
        judged.append(Freshness(d.bronze_table, newest, committed, age, d.stale_after_days))
    return judged

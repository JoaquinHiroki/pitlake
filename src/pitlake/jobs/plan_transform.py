import argparse
import json
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier
from pitlake.gate import THRESHOLDS, Threshold
from pitlake.silver import SILVER_SPECS, SilverSpec, parse_keys, transform_groups
from pitlake.tables import (
    QUALITY_RULES,
    QUALITY_RULES_SCHEMA,
    QUALITY_THRESHOLDS,
    QUALITY_THRESHOLDS_SCHEMA,
    cross_venue_ddl,
    cross_venue_log_ddl,
    quality_gate_results_ddl,
    quality_halts_ddl,
    quality_rules_ddl,
    quality_thresholds_ddl,
    quality_waivers_ddl,
    quarantine_ddl,
    rejection_rates_view,
    sequence_gaps_ddl,
    silver_commits_ddl,
    silver_ddl,
    silver_load_log_ddl,
)

TASK_VALUE_KEY = "groups"


def rule_rows(specs: list[SilverSpec], registered_at: datetime) -> list[tuple]:
    return [
        (s.table, position, r.name, r.action, r.condition, r.description, registered_at)
        for s in specs
        for position, r in enumerate(s.rules, start=1)
    ]


def threshold_rows(thresholds: tuple[Threshold, ...], registered_at: datetime) -> list[tuple]:
    return [
        (t.check, t.subject, float(t.max_rate), t.tolerance_rows, t.description, registered_at)
        for t in thresholds
    ]


def group_inputs(specs: list[SilverSpec], only: str = "") -> list[str]:
    chosen = parse_keys(only)
    inputs = []
    for group in transform_groups(specs):
        keys = [s.key_name for s in group if not chosen or s.key_name in chosen]
        if keys:
            inputs.append(",".join(keys))
    return inputs


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--datasets", default="", help="Dataset keys. Default: all")
    args = parser.parse_args(argv)
    catalog = validate_identifier(args.catalog)
    groups = group_inputs(list(SILVER_SPECS.values()), args.datasets)

    from pyspark.sql import SparkSession

    from pitlake.jobs.plan_ingest import set_task_value

    spark = SparkSession.builder.getOrCreate()
    specs = list(SILVER_SPECS.values())
    for ddl in (
        *(silver_ddl(catalog, s) for s in specs),
        quarantine_ddl(catalog),
        sequence_gaps_ddl(catalog),
        silver_load_log_ddl(catalog),
        quality_rules_ddl(catalog),
        quality_halts_ddl(catalog),
        silver_commits_ddl(catalog),
        cross_venue_ddl(catalog),
        cross_venue_log_ddl(catalog),
        quality_thresholds_ddl(catalog),
        quality_waivers_ddl(catalog),
        quality_gate_results_ddl(catalog),
    ):
        spark.sql(ddl)

    rows = rule_rows(specs, datetime.now(UTC))
    spark.createDataFrame(rows, schema=QUALITY_RULES_SCHEMA).write.mode("overwrite").saveAsTable(
        table_fqn(catalog, "control", QUALITY_RULES)
    )
    spark.sql(rejection_rates_view(catalog))
    spark.createDataFrame(
        threshold_rows(THRESHOLDS, datetime.now(UTC)), schema=QUALITY_THRESHOLDS_SCHEMA
    ).write.mode("overwrite").saveAsTable(table_fqn(catalog, "control", QUALITY_THRESHOLDS))

    set_task_value(TASK_VALUE_KEY, groups)
    print(f"OK: {len(rows)} rules, {len(specs)} Silver tables in {catalog}: {json.dumps(groups)}")


if __name__ == "__main__":
    main()

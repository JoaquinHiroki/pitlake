import argparse
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pitlake.config import (
    STAGING_VOLUME,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)
from pitlake.datasets import DATASETS
from pitlake.operations import (
    COST_LOOKBACK_DAYS,
    STAGING_DAYS,
    STAGING_LAYERS,
    costs_query,
    expired,
    freshness_query,
    judge_freshness,
    maintained,
    maintenance_query,
    retention_issues,
    staged_folders,
    tables_query,
)
from pitlake.tables import (
    FRESHNESS,
    FRESHNESS_SCHEMA,
    JOB_COSTS,
    JOB_COSTS_SCHEMA,
    RETENTION_PROPERTIES,
    TABLE_HEALTH,
    TABLE_HEALTH_SCHEMA,
    operations_ddls,
)

STEPS = ("costs", "storage", "freshness")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--step", required=True, choices=STEPS)
    parser.add_argument("--lookback-days", default=str(COST_LOOKBACK_DAYS))
    parser.add_argument("--staging-days", default=str(STAGING_DAYS))
    parser.add_argument("--enforce-freshness", default="false")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.enforce_freshness = args.enforce_freshness.strip().lower() == "true"
    args.lookback_days = int(args.lookback_days or COST_LOOKBACK_DAYS)
    args.staging_days = int(args.staging_days or STAGING_DAYS)
    if args.lookback_days < 1 or args.staging_days < 1:
        parser.error("--lookback-days and --staging-days must be at least 1")
    return args


def record_costs(spark, args, at: datetime) -> None:
    from pyspark.sql import functions as F

    since = at.date() - timedelta(days=args.lookback_days)
    query, params = costs_query(since)
    columns = [c.split()[0] for c in JOB_COSTS_SCHEMA.split(", ")]
    frame = (
        spark.sql(query, args=params)
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("list_usd", F.col("list_usd").cast("double"))
        .withColumn("recorded_at", F.lit(at))
        .withColumn("run_id", F.lit(args.run_id))
        .select(*columns)
    )
    table = table_fqn(args.catalog, "control", JOB_COSTS)
    frame.write.mode("overwrite").option("replaceWhere", f"usage_date >= '{since}'").saveAsTable(
        table
    )
    week = spark.sql(
        f"SELECT workload, round(sum(list_usd), 2) AS usd, round(sum(quantity), 2) AS quantity "
        f"FROM {table} WHERE usage_date >= :since GROUP BY workload ORDER BY usd DESC LIMIT 12",
        args={"since": at.date() - timedelta(days=7)},
    ).collect()
    print("list-price cost over the last 7 days:")
    for row in week:
        print(f"  {row['workload']:40} ${row['usd']:>8.2f}  ({row['quantity']} units)")
    print(f"OK: recorded costs since {since} in {table}")


def check_storage(spark, args, at: datetime) -> list[str]:
    query, params = maintenance_query(args.catalog)
    history: dict[tuple[str, str], dict] = {}
    for r in spark.sql(query, args=params).collect():
        entry = history.setdefault((r["schema_name"], r["table_name"]), {"last": {}, "failed": 0})
        entry["last"][r["operation_type"]] = r["last_run"]
        entry["failed"] += int(r["failed"])

    rows, problems = [], []
    for t in spark.sql(tables_query(args.catalog)).collect():
        schema, name = t["table_schema"], t["table_name"]
        if not maintained(name):
            continue
        detail = spark.sql(f"DESCRIBE DETAIL {table_fqn(args.catalog, schema, name)}").collect()[0]
        properties = dict(detail["properties"] or {})
        issues = retention_issues(schema, properties)
        problems += [f"{schema}.{name}: {i}" for i in issues]
        done = history.get((schema, name), {"last": {}, "failed": 0})
        rows.append(
            (
                at,
                args.run_id,
                schema,
                name,
                int(detail["numFiles"] or 0),
                int(detail["sizeInBytes"] or 0),
                list(detail["clusteringColumns"] or []),
                properties.get(RETENTION_PROPERTIES[0]),
                properties.get(RETENTION_PROPERTIES[1]),
                done["last"],
                done["failed"],
                issues,
            )
        )
    spark.createDataFrame(rows, schema=TABLE_HEALTH_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", TABLE_HEALTH)
    )
    size = sum(r[5] for r in rows) / 2**30
    print(f"{len(rows)} tables, {sum(r[4] for r in rows)} files, {size:.2f} GiB")

    now = time.time()
    for layer in STAGING_LAYERS:
        root = Path(volume_path(args.catalog, layer, STAGING_VOLUME))
        for folder in expired(staged_folders(root), now, args.staging_days):
            shutil.rmtree(folder)
            print(f"removed {folder}, untouched for over {args.staging_days} days")
    return problems


def check_freshness(spark, args, at: datetime) -> list[str]:
    found = {
        r["silver_table"]: (r["newest"], r["committed"])
        for r in spark.sql(freshness_query(args.catalog, DATASETS.values())).collect()
    }
    judged = judge_freshness(found, DATASETS.values(), at)
    spark.createDataFrame(
        [
            (
                at,
                args.run_id,
                f.silver_table,
                f.newest,
                f.committed,
                f.age_days,
                f.stale_after_days,
                f.stale,
            )
            for f in judged
        ],
        schema=FRESHNESS_SCHEMA,
    ).write.mode("append").saveAsTable(table_fqn(args.catalog, "control", FRESHNESS))
    for f in judged:
        age = "empty" if f.age_days is None else f"{f.age_days:.1f} days"
        print(
            f"{'STALE' if f.stale else 'fresh':5} {f.silver_table:28} {age:>11} "
            f"(tolerance {f.stale_after_days})"
        )
    return [f"{f.silver_table} is stale" for f in judged if f.stale]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in operations_ddls(args.catalog):
        spark.sql(ddl)
    at = datetime.now(UTC)

    if args.step == "costs":
        record_costs(spark, args, at)
        return
    if args.step == "freshness":
        stale = check_freshness(spark, args, at)
        if stale and args.enforce_freshness:
            raise RuntimeError(f"Silver is stale: {'; '.join(stale)}")
        print(f"OK: checked the freshness of {len(DATASETS)} Silver tables in {args.catalog}")
        return
    problems = check_storage(spark, args, at)
    if problems:
        raise RuntimeError(f"storage checks failed: {'; '.join(problems)}")
    print(f"OK: recorded the health of {args.catalog} and cleared expired staging")


if __name__ == "__main__":
    main()

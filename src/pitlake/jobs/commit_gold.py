import argparse
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.tables import GOLD_BUILDS, GOLD_COMMITS, GOLD_COMMITS_SCHEMA, GOLD_TABLES


def commit_rows(
    versions: dict[str, int], inputs: dict[str, dict], run_id: str, committed_at: datetime
) -> list[tuple]:
    missing = [t for t in GOLD_TABLES if t not in inputs or t not in versions]
    if missing:
        raise RuntimeError(f"run {run_id} built no {missing}; refusing a partial Gold commit")
    return [(run_id, committed_at, t, versions[t], inputs[t]) for t in GOLD_TABLES]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    catalog = validate_identifier(args.catalog)
    run_id = validate_segment(args.run_id.strip())

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    inputs = {
        r["gold_table"]: dict(r["inputs"])
        for r in spark.sql(
            f"SELECT gold_table, inputs FROM {table_fqn(catalog, 'control', GOLD_BUILDS)} "
            "WHERE run_id = :run_id",
            args={"run_id": run_id},
        ).collect()
    }
    versions = {
        t: spark.sql(f"DESCRIBE HISTORY {table_fqn(catalog, 'gold', t)} LIMIT 1").collect()[0][
            "version"
        ]
        for t in GOLD_TABLES
    }
    rows = commit_rows(versions, inputs, run_id, datetime.now(UTC))
    spark.createDataFrame(rows, schema=GOLD_COMMITS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(catalog, "control", GOLD_COMMITS)
    )
    for row in rows:
        print(f"committed gold.{row[2]} at version {row[3]}")
    print(f"OK: run {run_id} committed {len(rows)} Gold tables in {catalog}")


if __name__ == "__main__":
    main()

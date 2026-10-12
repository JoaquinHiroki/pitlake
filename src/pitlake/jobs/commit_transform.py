import argparse
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.silver import SILVER_SPECS
from pitlake.tables import (
    SHARED_SILVER_TABLES,
    SILVER_COMMITS,
    SILVER_COMMITS_SCHEMA,
    SILVER_LOAD_LOG,
    silver_commits_ddl,
)


def silver_tables() -> list[str]:
    return [*(s.table for s in SILVER_SPECS.values()), *SHARED_SILVER_TABLES]


def commit_rows(
    tables: list[str],
    versions: dict[str, int],
    processed: dict[str, tuple[int, int, int]],
    run_id: str,
    committed_at: datetime,
) -> list[tuple]:
    missing = [t for t in tables if t not in versions]
    if missing:
        raise RuntimeError(f"no Silver version for {missing}; refusing a partial commit")
    return [(run_id, committed_at, t, versions[t], *processed.get(t, (0, 0, 0))) for t in tables]


def latest_silver_commits_query(catalog: str) -> str:
    return (
        "SELECT silver_table, silver_version, committed_at, run_id "
        f"FROM {table_fqn(catalog, 'control', SILVER_COMMITS)} "
        "QUALIFY row_number() OVER (PARTITION BY silver_table ORDER BY committed_at DESC) = 1"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    catalog = validate_identifier(args.catalog)
    run_id = validate_segment(args.run_id.strip())

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(silver_commits_ddl(catalog))
    tables = silver_tables()

    versions = {
        t: spark.sql(f"DESCRIBE HISTORY {table_fqn(catalog, 'silver', t)} LIMIT 1").collect()[0][
            "version"
        ]
        for t in tables
    }
    processed = {
        r["silver_table"]: (r["files"], r["rows_out"], r["rows_quarantined"])
        for r in spark.sql(
            "SELECT silver_table, count(*) AS files, coalesce(sum(rows_out), 0) AS rows_out, "
            "coalesce(sum(rows_quarantined), 0) AS rows_quarantined "
            f"FROM {table_fqn(catalog, 'control', SILVER_LOAD_LOG)} WHERE run_id = :run_id "
            "GROUP BY silver_table",
            args={"run_id": run_id},
        ).collect()
    }

    rows = commit_rows(tables, versions, processed, run_id, datetime.now(UTC))
    spark.createDataFrame(rows, schema=SILVER_COMMITS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(catalog, "control", SILVER_COMMITS)
    )
    for row in rows:
        print(f"committed {row[2]} at Silver version {row[3]} ({row[4]} files this run)")
    print(f"OK: run {run_id} committed {len(rows)} Silver tables in {catalog}")


if __name__ == "__main__":
    main()

"""Ingest task 4: advance the record of what has been processed, only if every source succeeded.

The job runs this task only when every collect and load iteration succeeded (run_if ALL_SUCCESS).
It appends one row per registered dataset to control.ingest_commits with the Bronze table's
current Delta version. Downstream layers read each Bronze table AS OF its latest committed version,
so files that a partly failed run did load stay invisible until a run in which every source
succeeds (ADR 0005). The rows are written in one append, so a commit is all datasets or none.
"""

import argparse
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.datasets import DATASETS, Dataset
from pitlake.tables import (
    BRONZE_LOAD_LOG,
    INGEST_COMMITS,
    INGEST_COMMITS_SCHEMA,
    ingest_commits_ddl,
)


def commit_rows(
    datasets: list[Dataset],
    versions: dict[str, int],
    loaded: dict[str, tuple[int, int]],
    run_id: str,
    committed_at: datetime,
) -> list[tuple]:
    """One row per dataset. `loaded` maps a dataset key to this run's (files, rows)."""
    missing = [d.key for d in datasets if d.key not in versions]
    if missing:
        raise RuntimeError(f"no Bronze version for {missing}; refusing a partial commit")
    return [
        (
            run_id,
            committed_at,
            d.source,
            d.name,
            d.bronze_table,
            versions[d.key],
            *loaded.get(d.key, (0, 0)),
        )
        for d in datasets
    ]


def latest_commits_query(catalog: str) -> str:
    """The Bronze version each dataset is committed at. Downstream layers read through this."""
    return (
        "SELECT source, dataset, bronze_table, bronze_version, committed_at, run_id "
        f"FROM {table_fqn(catalog, 'control', INGEST_COMMITS)} "
        "QUALIFY row_number() OVER (PARTITION BY source, dataset ORDER BY committed_at DESC) = 1"
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
    spark.sql(ingest_commits_ddl(catalog))
    datasets = list(DATASETS.values())

    versions = {
        d.key: spark.sql(
            f"DESCRIBE HISTORY {table_fqn(catalog, 'bronze', d.bronze_table)} LIMIT 1"
        ).collect()[0]["version"]
        for d in datasets
    }
    loaded = {
        f"{r['source']}.{r['dataset']}": (r["files"], r["rows"])
        for r in spark.sql(
            "SELECT source, dataset, count(*) AS files, coalesce(sum(row_count), 0) AS rows "
            f"FROM {table_fqn(catalog, 'control', BRONZE_LOAD_LOG)} WHERE run_id = :run_id "
            "GROUP BY source, dataset",
            args={"run_id": run_id},
        ).collect()
    }

    rows = commit_rows(datasets, versions, loaded, run_id, datetime.now(UTC))
    spark.createDataFrame(rows, schema=INGEST_COMMITS_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(catalog, "control", INGEST_COMMITS)
    )
    for row in rows:
        print(f"committed {row[2]}.{row[3]} at Bronze version {row[5]} ({row[6]} files this run)")
    print(f"OK: run {run_id} committed {len(rows)} datasets in {catalog}")


if __name__ == "__main__":
    main()

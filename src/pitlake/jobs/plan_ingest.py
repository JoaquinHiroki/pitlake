import argparse
import json
from datetime import UTC, datetime

from pitlake.config import table_fqn, validate_identifier
from pitlake.datasets import DATASETS, Dataset
from pitlake.tables import (
    SOURCE_REGISTRY,
    SOURCE_REGISTRY_SCHEMA,
    bronze_ddl,
    bronze_load_log_ddl,
    ingest_commits_ddl,
    landing_manifest_ddl,
    source_registry_ddl,
)

TASK_VALUE_KEY = "datasets"


def registry_rows(datasets: list[Dataset], registered_at: datetime) -> list[tuple]:
    return [
        (
            d.source,
            d.name,
            d.bronze_table,
            d.archive,
            d.file_format,
            d.description,
            registered_at,
        )
        for d in datasets
    ]


def dataset_keys(datasets: list[Dataset], only: str = "") -> list[str]:
    keys = [d.key for d in datasets]
    wanted = [k.strip() for k in only.split(",") if k.strip()]
    unknown = sorted(set(wanted) - set(keys))
    if unknown:
        raise ValueError(f"unknown datasets {unknown}; registered: {keys}")
    return [k for k in keys if k in wanted] if wanted else keys


def set_task_value(key: str, value: object) -> None:
    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key=key, value=value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--datasets", default="")
    args = parser.parse_args(argv)
    catalog = validate_identifier(args.catalog)
    keys = dataset_keys(list(DATASETS.values()), args.datasets)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    datasets = list(DATASETS.values())
    for ddl in (
        landing_manifest_ddl(catalog),
        bronze_load_log_ddl(catalog),
        source_registry_ddl(catalog),
        ingest_commits_ddl(catalog),
        *(bronze_ddl(catalog, d) for d in datasets),
    ):
        spark.sql(ddl)

    rows = registry_rows(datasets, datetime.now(UTC))
    spark.createDataFrame(rows, schema=SOURCE_REGISTRY_SCHEMA).write.mode("overwrite").saveAsTable(
        table_fqn(catalog, "control", SOURCE_REGISTRY)
    )

    set_task_value(TASK_VALUE_KEY, keys)
    print(f"OK: {len(keys)} datasets registered in {catalog}: {json.dumps(keys)}")


if __name__ == "__main__":
    main()

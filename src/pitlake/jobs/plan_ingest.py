"""Ingest task 0: publish the source registry and hand the job its list of datasets.

The registry in pitlake.datasets is the single source of truth. This task writes it to
control.source_registry, where downstream layers check that a source is registered (spec section
5), and passes the dataset keys to the job as the task value `datasets`, which the collect and load
for_each tasks iterate over (ADR 0005). Adding a dataset therefore changes no job definition.

It also creates every control and Bronze table up front, so the parallel iterations that follow
never race to create the same table.
"""

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


def dataset_keys(datasets: list[Dataset]) -> list[str]:
    """The for_each inputs, in registry order."""
    return [d.key for d in datasets]


def set_task_value(key: str, value: object) -> None:
    from databricks.sdk.runtime import dbutils

    dbutils.jobs.taskValues.set(key=key, value=value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    catalog = validate_identifier(parser.parse_args(argv).catalog)

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

    keys = dataset_keys(datasets)
    set_task_value(TASK_VALUE_KEY, keys)
    print(f"OK: {len(keys)} datasets registered in {catalog}: {json.dumps(keys)}")


if __name__ == "__main__":
    main()

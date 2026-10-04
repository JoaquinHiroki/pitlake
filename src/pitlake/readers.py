"""Read one landed data file into Bronze's shape: the dataset's columns as STRING plus
_corrupt_record. The reader is chosen by the dataset's file_format, never by its source.

Records that do not parse are kept, not dropped: their raw text goes into _corrupt_record and the
other columns are null, so Silver can count and explain them.
"""

from pitlake.datasets import Dataset
from pitlake.tables import CORRUPT_RECORD_COLUMN, raw_schema


def read_raw(spark, dataset: Dataset, path: str):
    if dataset.file_format == "csv":
        return _read_csv(spark, dataset, path)
    if dataset.file_format == "json_rows":
        return _read_json_rows(spark, dataset, path)
    raise ValueError(f"no reader for file_format {dataset.file_format!r}")


def _read_csv(spark, dataset: Dataset, path: str):
    return (
        spark.read.schema(raw_schema(dataset))
        .option("header", str(dataset.header).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .csv(path)
    )


def _read_json_rows(spark, dataset: Dataset, path: str):
    """Each line is one API response, an array of rows; each row is an array of values.

    Values are read as STRING, so a JSON number keeps the exact text the source sent.
    """
    from pyspark.sql import functions as F

    width = len(dataset.columns)
    nothing = F.lit(None).cast("string")
    lines = spark.read.text(path).select(
        "value", F.from_json("value", "array<array<string>>").alias("rows")
    )

    rows = lines.where(F.col("rows").isNotNull()).select(F.explode("rows").alias("row"))
    # size() of a null row is null, so the condition is not true and the row counts as corrupt.
    well_formed = F.size("row") == width
    parsed = rows.select(
        *(F.when(well_formed, F.get("row", i)).alias(c) for i, c in enumerate(dataset.columns)),
        F.when(well_formed, nothing)
        .otherwise(F.coalesce(F.to_json("row"), F.lit("null")))
        .alias(CORRUPT_RECORD_COLUMN),
    )
    unparsed = lines.where(F.col("rows").isNull()).select(
        *(nothing.alias(c) for c in dataset.columns),
        F.col("value").alias(CORRUPT_RECORD_COLUMN),
    )
    # Call the method on the instance: on serverless these are Spark Connect DataFrames.
    return parsed.unionByName(unparsed)

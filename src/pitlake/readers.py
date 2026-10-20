from functools import reduce

from pitlake.datasets import XBRL_PATH_COLUMNS, Dataset
from pitlake.tables import CORRUPT_RECORD_COLUMN, raw_schema


def read_raw(spark, dataset: Dataset, path: str):
    if dataset.file_format == "csv":
        return _read_csv(spark, dataset, path)
    if dataset.file_format == "json_rows":
        return _read_json_rows(spark, dataset, path)
    if dataset.file_format == "json_records":
        return _read_json_records(spark, dataset, path)
    if dataset.file_format == "json_columns":
        return _read_json_columns(spark, dataset, path)
    if dataset.file_format == "xbrl_facts":
        return _read_xbrl_facts(spark, dataset, path)
    if dataset.file_format == "document":
        return _read_document(spark, dataset, path)
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
    from pyspark.sql import functions as F

    width = len(dataset.columns)
    nothing = F.lit(None).cast("string")
    lines = spark.read.text(path).select(
        "value", F.from_json("value", "array<array<string>>").alias("rows")
    )

    rows = lines.where(F.col("rows").isNotNull()).select(F.explode("rows").alias("row"))
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
    return parsed.unionByName(unparsed)


def records_schema(dataset: Dataset) -> str:
    fields = ", ".join(f"`{f}`: STRING" for f in dataset.fields)
    return f"STRUCT<`{dataset.records_path}`: ARRAY<STRUCT<{fields}>>>"


def _read_json_records(spark, dataset: Dataset, path: str):
    from pyspark.sql import functions as F

    nothing = F.lit(None).cast("string")
    lines = spark.read.text(path).select(
        "value", F.from_json("value", records_schema(dataset)).alias("doc")
    )
    records = F.col("doc").getField(dataset.records_path)

    parsed = (
        lines.where(records.isNotNull())
        .select(F.explode(records).alias("record"))
        .select(
            *(
                F.col("record").getField(f).alias(c)
                for f, c in zip(dataset.fields, dataset.columns, strict=True)
            ),
            F.when(F.col("record").isNull(), F.lit("null"))
            .otherwise(nothing)
            .alias(CORRUPT_RECORD_COLUMN),
        )
    )
    unparsed = lines.where(records.isNull()).select(
        *(nothing.alias(c) for c in dataset.columns),
        F.col("value").alias(CORRUPT_RECORD_COLUMN),
    )
    return parsed.unionByName(unparsed)


def columns_schema(dataset: Dataset) -> str:
    schema = "STRUCT<" + ", ".join(f"`{f}`: ARRAY<STRING>" for f in dataset.fields) + ">"
    for key in reversed(dataset.records_path.split(".")):
        schema = f"STRUCT<`{key}`: {schema}>"
    return schema


def _read_json_columns(spark, dataset: Dataset, path: str):
    from pyspark.sql import functions as F

    nothing = F.lit(None).cast("string")
    lines = spark.read.text(path).select(
        "value", F.from_json("value", columns_schema(dataset)).alias("doc")
    )
    path = dataset.records_path.split(".")
    table = reduce(lambda col, key: col.getField(key), path, F.col("doc"))
    arrays = [table.getField(f) for f in dataset.fields]
    aligned = F.coalesce(
        reduce(lambda a, b: a & b, (F.size(a) == F.size(arrays[0]) for a in arrays)), F.lit(False)
    )
    records = F.transform(
        arrays[0],
        lambda _, i: F.struct(
            *(F.get(a, i).alias(c) for a, c in zip(arrays, dataset.columns, strict=True))
        ),
    )
    parsed = (
        lines.where(aligned)
        .select(F.explode(records).alias("record"))
        .select(
            *(F.col("record").getField(c).alias(c) for c in dataset.columns),
            nothing.alias(CORRUPT_RECORD_COLUMN),
        )
    )
    unparsed = lines.where(~aligned).select(
        *(nothing.alias(c) for c in dataset.columns),
        F.col("value").alias(CORRUPT_RECORD_COLUMN),
    )
    return parsed.unionByName(unparsed)


def xbrl_facts_schema(dataset: Dataset) -> str:
    record = ", ".join(f"`{f}`: STRING" for f in dataset.fields[len(XBRL_PATH_COLUMNS) :])
    return (
        "STRUCT<`facts`: MAP<STRING, MAP<STRING, "
        f"STRUCT<`units`: MAP<STRING, ARRAY<STRUCT<{record}>>>>>>>"
    )


def _read_xbrl_facts(spark, dataset: Dataset, path: str):
    from pyspark.sql import functions as F

    nothing = F.lit(None).cast("string")
    taxonomy, concept, unit = XBRL_PATH_COLUMNS
    width = len(XBRL_PATH_COLUMNS)
    lines = spark.read.text(path).select(
        "value", F.from_json("value", xbrl_facts_schema(dataset)).alias("doc")
    )
    facts = F.col("doc").getField("facts")

    parsed = (
        lines.where(facts.isNotNull())
        .select(F.explode(facts).alias(taxonomy, "concepts"))
        .select(taxonomy, F.explode("concepts").alias(concept, "entry"))
        .select(
            taxonomy, concept, F.explode(F.col("entry").getField("units")).alias(unit, "records")
        )
        .select(taxonomy, concept, unit, F.explode("records").alias("record"))
        .select(
            taxonomy,
            concept,
            unit,
            *(
                F.col("record").getField(f).alias(c)
                for f, c in zip(dataset.fields[width:], dataset.columns[width:], strict=True)
            ),
            F.when(F.col("record").isNull(), F.lit("null"))
            .otherwise(nothing)
            .alias(CORRUPT_RECORD_COLUMN),
        )
    )
    unparsed = lines.where(facts.isNull()).select(
        *(nothing.alias(c) for c in dataset.columns),
        F.col("value").alias(CORRUPT_RECORD_COLUMN),
    )
    return parsed.unionByName(unparsed)


def _read_document(spark, dataset: Dataset, path: str):
    from pyspark.sql import functions as F

    whole = spark.read.option("wholetext", "true").text(path)
    readable = F.coalesce(F.length("value") > 0, F.lit(False))
    return whole.select(
        F.when(readable, F.col("value")).alias(dataset.columns[0]),
        F.when(readable, F.lit(None).cast("string"))
        .otherwise(F.coalesce(F.col("value"), F.lit("")))
        .alias(CORRUPT_RECORD_COLUMN),
    )

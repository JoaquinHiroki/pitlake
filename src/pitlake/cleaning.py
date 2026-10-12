from collections.abc import Mapping
from datetime import datetime
from functools import reduce

from pitlake.datasets import Dataset
from pitlake.quality import (
    DROP,
    DUPLICATE_KEY,
    KNOWABLE_AT_BEFORE_EVENT,
    KNOWABLE_AT_MISSING,
    NO_FAULT,
    SEQUENCE_CONTINUOUS,
    fault_target_sql,
    first_failure_sql,
)
from pitlake.silver import SILVER_METADATA_COLUMNS, SilverSpec
from pitlake.tables import CORRUPT_RECORD_COLUMN

FAILED_RULE = "_failed_rule"
RECORD = "_record"
_RAW = "_raw"
_BRONZE_METADATA = (
    "_source",
    "_dataset",
    "_symbol",
    "_period_start",
    "_source_file",
    "_source_sha256",
    "_ingested_at",
)


def _raw_struct(dataset: Dataset):
    from pyspark.sql import functions as F

    return F.struct(
        *(F.col(f"`{c}`") for c in dataset.columns), F.col(CORRUPT_RECORD_COLUMN)
    ).alias(_RAW)


def prepare(
    spec: SilverSpec,
    bronze,
    registry,
    lookups: Mapping[str, object] | None = None,
    fault: str = NO_FAULT,
):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = bronze.select(
        *(F.expr(c.expr).alias(c.name) for c in spec.columns),
        _raw_struct(spec.dataset),
        F.col(CORRUPT_RECORD_COLUMN),
        *(F.col(c) for c in _BRONZE_METADATA),
    )

    registered = (
        registry.select(
            F.col("source").alias("_registered_source"),
            F.col("dataset").alias("_registered_dataset"),
        )
        .distinct()
        .withColumn("_registered", F.lit(True))
    )
    df = df.join(
        F.broadcast(registered),
        (F.col("_source") == F.col("_registered_source"))
        & (F.col("_dataset") == F.col("_registered_dataset")),
        "left",
    ).drop("_registered_source", "_registered_dataset")

    for lookup in spec.lookups:
        if lookups is None or lookup.table not in lookups:
            raise ValueError(f"{spec.table} needs the lookup table {lookup.table}")
        right_keys = [f"_lookup_{theirs}" for _, theirs in lookup.on]
        right = (
            lookups[lookup.table]
            .select(
                *(
                    F.col(theirs).alias(k)
                    for (_, theirs), k in zip(lookup.on, right_keys, strict=True)
                ),
                *(F.col(c.expr).alias(c.name) for c in lookup.columns),
            )
            .dropDuplicates(right_keys)
        )
        condition = reduce(
            lambda a, b: a & b,
            (F.col(ours) == F.col(k) for (ours, _), k in zip(lookup.on, right_keys, strict=True)),
        )
        df = df.join(right, condition, "left").drop(*right_keys)

    for column in spec.derived:
        df = df.withColumn(column.name, F.expr(column.expr))
    df = df.withColumn("event_time", F.expr(spec.event_time).cast("timestamp"))
    df = df.withColumn("knowable_at", F.expr(spec.knowable_at).cast("timestamp"))

    if fault != NO_FAULT:
        target = F.coalesce(F.expr(fault_target_sql(spec.rules)), F.lit(False))
        first = Window.partitionBy("_source_file", target).orderBy("_ingested_at")
        hit = target & (F.row_number().over(first) == 1)
        if fault == KNOWABLE_AT_MISSING:
            corrupted = F.lit(None).cast("timestamp")
        elif fault == KNOWABLE_AT_BEFORE_EVENT:
            corrupted = F.col("event_time") - F.expr("INTERVAL 1 SECOND")
        else:
            raise ValueError(f"unknown fault {fault!r}")
        df = df.withColumn("knowable_at", F.when(hit, corrupted).otherwise(F.col("knowable_at")))
    return df


def evaluate(spec: SilverSpec, prepared):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = prepared.withColumn(FAILED_RULE, F.expr(first_failure_sql(spec.rules)))
    passing = F.col(FAILED_RULE).isNull()
    within = ["_source_file"] if spec.scope == "file" else []
    # Duplicates are judged only among rows that pass every other rule.
    copies = Window.partitionBy(*within, *spec.key, passing).orderBy(
        "_period_start", "_source_file", _RAW
    )
    df = df.withColumn(
        FAILED_RULE,
        F.when(passing & (F.row_number().over(copies) > 1), F.lit(DUPLICATE_KEY)).otherwise(
            F.col(FAILED_RULE)
        ),
    )
    return df.withColumn(RECORD, F.when(F.col(FAILED_RULE).isNotNull(), F.to_json(_RAW))).drop(_RAW)


def silver_rows(spec: SilverSpec, evaluated, bronze_version: int, run_id: str, at: datetime):
    from pyspark.sql import functions as F

    return evaluated.where(F.col(FAILED_RULE).isNull()).select(
        *(F.col(c.name).cast(c.type).alias(c.name) for c in spec.output_columns),
        *_metadata(bronze_version, run_id, at),
    )


def _metadata(bronze_version: int, run_id: str, at: datetime) -> list:
    from pyspark.sql import functions as F

    values = {
        "_bronze_version": F.lit(bronze_version).cast("bigint"),
        "_transformed_at": F.lit(at).cast("timestamp"),
        "_transform_run_id": F.lit(run_id),
    }
    return [values.get(name, F.col(name)).alias(name) for name, _ in SILVER_METADATA_COLUMNS]


def quarantine_rows(spec: SilverSpec, evaluated, bronze_version: int, run_id: str, at: datetime):
    from pyspark.sql import functions as F

    drop_rules = [r.name for r in spec.rules if r.action == DROP]
    return evaluated.where(F.col(FAILED_RULE).isin(drop_rules)).select(
        F.lit(spec.table).alias("silver_table"),
        F.col(FAILED_RULE).alias("rule"),
        F.col(RECORD).alias("record"),
        F.col("event_time"),
        F.col("knowable_at"),
        *_metadata(bronze_version, run_id, at),
    )


def sequence_gaps(spec: SilverSpec, evaluated, run_id: str, at: datetime):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    sequence = spec.sequence
    if sequence is None:
        return None
    step = sequence.step
    group = ["_source_file", "_symbol", "_period_start"]
    values = evaluated.where(F.col(FAILED_RULE).isNull()).select(
        *group, F.expr(sequence.value).cast("bigint").alias("_value")
    )

    previous = F.lag("_value").over(Window.partitionBy(*group).orderBy("_value"))
    frames = [
        values.withColumn("_previous", previous)
        .where(F.col("_value") - F.col("_previous") > step)
        .select(
            *group,
            (F.col("_previous") + step).alias("gap_start"),
            (F.col("_value") - step).alias("gap_end"),
        )
    ]
    if sequence.first or sequence.last:
        bounds = values.groupBy(*group).agg(
            F.min("_value").alias("_min"), F.max("_value").alias("_max")
        )
        if sequence.first:
            first = F.expr(sequence.first).cast("bigint")
            frames.append(
                bounds.where(F.col("_min") > first).select(
                    *group, first.alias("gap_start"), (F.col("_min") - step).alias("gap_end")
                )
            )
        if sequence.last:
            last = F.expr(sequence.last).cast("bigint")
            frames.append(
                bounds.where(F.col("_max") < last).select(
                    *group, (F.col("_max") + step).alias("gap_start"), last.alias("gap_end")
                )
            )
    gaps = reduce(lambda a, b: a.unionByName(b), frames)
    return gaps.select(
        F.lit(spec.table).alias("silver_table"),
        F.lit(SEQUENCE_CONTINUOUS).alias("rule"),
        F.lit(sequence.value).alias("sequence"),
        "_symbol",
        "_period_start",
        "_source_file",
        "gap_start",
        "gap_end",
        F.expr(f"(gap_end - gap_start) div {int(step)} + 1").alias("missing"),
        F.lit(at).cast("timestamp").alias("_recorded_at"),
        F.lit(run_id).alias("_transform_run_id"),
    )

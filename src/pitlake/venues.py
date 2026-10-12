from datetime import date, datetime

from pitlake.gate import VenuePair
from pitlake.quality import NO_FAULT, VENUE_MISSING_MINUTES, VENUE_PRICE_DIVERGENCE

AGREE = "agree"
PRICE_DIVERGENCE = "price_divergence"
MISSING_LEFT = "missing_left"
MISSING_RIGHT = "missing_right"
MISSING_BOTH = "missing_both"
FAULT_HOUR = 12
PRICE = "decimal(28,10)"
VOLUME = "decimal(38,10)"
COMPARISON_VALUES = (
    *(("left", c, PRICE) for c in ("open", "high", "low", "close")),
    ("left", "volume", VOLUME),
    ("left", "trades", "bigint"),
    *(("right", c, PRICE) for c in ("open", "high", "low", "close")),
    ("right", "volume", VOLUME),
)
FAULT_PRICE_FACTOR = 1.05


def minute_bars(trades, symbol: str, days: list[date]):
    from pyspark.sql import functions as F

    order = F.struct("event_time", "trade_id")
    return (
        trades.where((F.col("symbol") == symbol) & F.col("_period_start").isin(days))
        .groupBy(F.date_trunc("minute", "event_time").alias("event_time"))
        .agg(
            F.min_by("price", order).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order).alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trades"),
        )
    )


def minute_candles(candles, symbol: str, days: list[date]):
    from pyspark.sql import functions as F

    return candles.where((F.col("symbol") == symbol) & F.col("_period_start").isin(days)).select(
        "event_time", "open", "high", "low", "close", "volume"
    )


def compare(spark, pair: VenuePair, left, right, days: list[date], fault: str = NO_FAULT):
    from pyspark.sql import functions as F

    grid = spark.createDataFrame([(d,) for d in days], "day DATE").select(
        "day",
        F.explode(
            F.expr(
                "sequence(CAST(day AS TIMESTAMP), CAST(day AS TIMESTAMP) + INTERVAL 1439 MINUTES, "
                "INTERVAL 1 MINUTE)"
            )
        ).alias("event_time"),
    )
    in_fault_hour = F.hour("event_time") == FAULT_HOUR
    if fault == VENUE_MISSING_MINUTES:
        right = right.where(~in_fault_hour)
    elif fault == VENUE_PRICE_DIVERGENCE:
        right = right.withColumn(
            "close",
            F.when(in_fault_hour, F.col("close") * FAULT_PRICE_FACTOR)
            .otherwise(F.col("close"))
            .cast(PRICE),
        )
    elif fault != NO_FAULT:
        raise ValueError(f"unknown venue fault {fault!r}")

    lefts = left.select("event_time", *(F.col(c).alias(f"left_{c}") for c in left.columns[1:]))
    rights = right.select("event_time", *(F.col(c).alias(f"right_{c}") for c in right.columns[1:]))
    joined = grid.join(lefts, "event_time", "left").join(rights, "event_time", "left")
    diff = ((F.col("left_close") - F.col("right_close")) / F.col("right_close") * 10000).cast(
        "double"
    )
    left_in, right_in = F.col("left_close").isNotNull(), F.col("right_close").isNotNull()
    status = (
        F.when(left_in & right_in & (F.abs(diff) > pair.max_close_diff_bps), PRICE_DIVERGENCE)
        .when(left_in & right_in, AGREE)
        .when(left_in, MISSING_RIGHT)
        .when(right_in, MISSING_LEFT)
        .otherwise(MISSING_BOTH)
    )
    return joined.select(
        F.lit(pair.name).alias("pair"),
        "day",
        "event_time",
        (F.col("event_time") + F.expr("INTERVAL 1 MINUTE")).alias("knowable_at"),
        status.alias("status"),
        diff.alias("close_diff_bps"),
        *(
            F.col(f"{side}_{c}").cast(kind).alias(f"{side}_{c}")
            for side, c, kind in COMPARISON_VALUES
        ),
    )


def daily_counts(compared):
    from pyspark.sql import functions as F

    return compared.groupBy("day").agg(
        F.count("*").alias("minutes"),
        F.count_if(F.col("status") == PRICE_DIVERGENCE).alias("divergent"),
        F.count_if(F.col("status") == MISSING_LEFT).alias("missing_left"),
        F.count_if(F.col("status") == MISSING_RIGHT).alias("missing_right"),
    )


def comparison_rows(spark, compared, files: dict, run_id: str, at: datetime):
    from pyspark.sql import functions as F

    spark_files = spark.createDataFrame(
        [(d, f.left_file, f.right_file) for d, f in files.items()],
        "day DATE, _left_file STRING, _right_file STRING",
    )
    return compared.join(F.broadcast(spark_files), "day", "left").select(
        *compared.columns,
        "_left_file",
        "_right_file",
        F.lit(at).cast("timestamp").alias("_compared_at"),
        F.lit(run_id).alias("_run_id"),
    )

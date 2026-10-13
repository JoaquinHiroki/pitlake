from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pitlake.config import table_fqn
from pitlake.gold import GLOBAL_ENTITY
from pitlake.snapshots import readable_version
from pitlake.tables import FEATURES, GOLD_COMMITS

AS_OF = "as_of"
RESULT_COLUMNS = (
    "entity",
    AS_OF,
    "feature",
    "value",
    "event_time",
    "knowable_at",
    "source_table",
    "source_ref",
)


def require_instant(at: datetime) -> datetime:
    if not isinstance(at, datetime) or at.tzinfo is None:
        raise TypeError(
            f"a feature is only defined at an instant: pass a timezone-aware datetime, not {at!r}"
        )
    return at


def as_of(features, spine):
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType
    from pyspark.sql.window import Window

    fields = {f.name: f.dataType for f in spine.schema.fields}
    if "entity" not in fields or AS_OF not in fields:
        raise ValueError(f"the spine needs entity and {AS_OF} columns, got {sorted(fields)}")
    if not isinstance(fields[AS_OF], TimestampType):
        raise TypeError(f"{AS_OF} must be a TIMESTAMP, got {fields[AS_OF].simpleString()}")

    keys = spine.select("entity", AS_OF).distinct()
    entities = keys.select("entity").distinct()
    columns = ["entity", "feature", "event_time", "knowable_at", "source_table", "source_ref"]
    specific = features.where(F.col("entity") != GLOBAL_ENTITY).select(*columns, "value")
    shared = (
        features.where(F.col("entity") == GLOBAL_ENTITY)
        .drop("entity")
        .crossJoin(entities)
        .select(*columns, "value")
    )
    rows = specific.unionByName(shared)

    def winner(event_time, knowable_at, source_table, source_ref, value):
        return F.struct(
            event_time.cast("timestamp").alias("event_time"),
            knowable_at.cast("timestamp").alias("knowable_at"),
            source_table.cast("string").alias("source_table"),
            source_ref.cast("string").alias("source_ref"),
            value.cast("double").alias("value"),
        )

    facts = rows.select(
        "entity",
        "feature",
        F.col("knowable_at").alias("_t"),
        F.lit(0).alias("_probe"),
        winner(*(F.col(c) for c in columns[2:]), F.col("value")).alias("_row"),
    )
    none = F.lit(None)
    probes = keys.join(rows.select("entity", "feature").distinct(), "entity").select(
        "entity",
        "feature",
        F.col(AS_OF).alias("_t"),
        F.lit(1).alias("_probe"),
        # A struct of nulls is not NULL: it would win the running maximum when nothing is known.
        F.when(F.lit(False), winner(none, none, none, none, none)).alias("_row"),
    )
    # Latest event first, then the latest revision of it, among rows already knowable.
    known = (
        Window.partitionBy("entity", "feature")
        .orderBy(F.col("_t").asc_nulls_first(), "_probe")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        facts.unionByName(probes)
        .withColumn("_best", F.max("_row").over(known))
        .where((F.col("_probe") == 1) & F.col("_t").isNotNull() & F.col("_best").isNotNull())
        .select(
            "entity",
            F.col("_t").alias(AS_OF),
            "feature",
            *(F.col(f"_best.{c}").alias(c) for c in ("value", "event_time", "knowable_at")),
            *(F.col(f"_best.{c}").alias(c) for c in ("source_table", "source_ref")),
        )
    )


@dataclass(frozen=True)
class FeaturesSnapshot:
    committed_version: int
    version: int
    commit_run_id: str
    inputs: dict


def latest_gold_commits_query(catalog: str) -> str:
    return (
        "SELECT gold_table, gold_version, committed_at, run_id, inputs "
        f"FROM {table_fqn(catalog, 'control', GOLD_COMMITS)} "
        "QUALIFY row_number() OVER (PARTITION BY gold_table ORDER BY committed_at DESC) = 1"
    )


def committed_snapshot(spark, catalog: str) -> FeaturesSnapshot:
    commits = {r["gold_table"]: r for r in spark.sql(latest_gold_commits_query(catalog)).collect()}
    if FEATURES not in commits:
        raise RuntimeError(f"{catalog} has no committed Gold features yet")
    commit = commits[FEATURES]
    version = readable_version(spark, table_fqn(catalog, "gold", FEATURES), commit["gold_version"])
    return FeaturesSnapshot(
        commit["gold_version"], version, commit["run_id"], dict(commit["inputs"] or {})
    )


def features_version(spark, catalog: str, version: int):
    table = table_fqn(catalog, "gold", FEATURES)
    return spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(version)}")


def committed_features(spark, catalog: str):
    return features_version(spark, catalog, committed_snapshot(spark, catalog).version)


def read_features(
    spark, catalog: str, spine, names: Iterable[str] | None = None, version: int | None = None
):
    from pyspark.sql import functions as F

    if version is None:
        features = committed_features(spark, catalog)
    else:
        features = features_version(spark, catalog, version)
    if names is not None:
        features = features.where(F.col("feature").isin(*names))
    return as_of(features, spine)


def features_at(spark, catalog: str, entities: Iterable[str], at: datetime, names=None):
    at = require_instant(at)
    spine = spark.createDataFrame(
        [(e, at) for e in sorted(set(entities))], f"entity STRING, {AS_OF} TIMESTAMP"
    )
    return read_features(spark, catalog, spine, names)

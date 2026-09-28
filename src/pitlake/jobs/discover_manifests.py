"""Ingest task 1: record every new landing manifest in control.landing_manifest.

Auto Loader tracks which manifest files it has already seen in its checkpoint, so each run
picks up only what the collector wrote since the last one.
"""

import argparse

from pitlake.config import (
    CHECKPOINTS_VOLUME,
    MANIFEST_SUFFIX,
    landing_path,
    table_fqn,
    validate_identifier,
    volume_path,
)
from pitlake.manifest import SPARK_SCHEMA
from pitlake.tables import LANDING_MANIFEST, landing_manifest_ddl


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    catalog = validate_identifier(parser.parse_args().catalog)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.sql(landing_manifest_ddl(catalog))

    checkpoint = f"{volume_path(catalog, 'control', CHECKPOINTS_VOLUME)}/{LANDING_MANIFEST}"
    manifests = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", f"*{MANIFEST_SUFFIX}")
        # A re-fetched file rewrites its manifest; it must be seen again with the new checksum.
        .option("cloudFiles.allowOverwrites", "true")
        .schema(SPARK_SCHEMA)
        .load(landing_path(catalog))
        .select(
            "*",
            F.col("_metadata.file_path").alias("_manifest_file"),
            F.current_timestamp().alias("_discovered_at"),
        )
    )
    query = (
        manifests.writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table_fqn(catalog, "control", LANDING_MANIFEST))
    )
    query.awaitTermination()
    # availableNow may split the backlog into several micro-batches; lastProgress is only the last.
    discovered = sum(p.get("numInputRows", 0) for p in query.recentProgress)
    print(f"OK: discovered {discovered} new manifests in {catalog}")


if __name__ == "__main__":
    main()

"""Deployment smoke test: fails the job if the deployed platform is not usable."""

import argparse
import os
from importlib.metadata import version

from pitlake.config import landing_path, missing_layers, schema_fqn, validate_identifier


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    catalog = validate_identifier(parser.parse_args().catalog)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    existing = {row[0] for row in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()}
    missing = missing_layers(existing)
    if missing:
        raise SystemExit(f"{catalog}: missing schemas {missing}")

    volume = landing_path(catalog)
    if not os.path.isdir(volume):
        raise SystemExit(f"landing volume not found at {volume}")

    control = schema_fqn(catalog, "control")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {control}.deploy_log "
        "(checked_at TIMESTAMP, catalog STRING, package_version STRING)"
    )
    pkg_version = version("pitlake")
    spark.sql(
        f"INSERT INTO {control}.deploy_log "
        f"VALUES (current_timestamp(), '{catalog}', '{pkg_version}')"
    )
    print(f"OK: {catalog} has all layers, landing volume and a writable control schema")


if __name__ == "__main__":
    main()

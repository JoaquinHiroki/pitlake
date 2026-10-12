import os

import pytest

REQUIRED = os.environ.get("PITLAKE_REQUIRE_SPARK") == "1"


@pytest.fixture(scope="session")
def spark():
    if not REQUIRED:
        pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("pitlake-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

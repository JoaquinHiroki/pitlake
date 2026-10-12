SAME_DATA_OPERATIONS = frozenset(
    {
        "OPTIMIZE",
        "VACUUM START",
        "VACUUM END",
        "ANALYZE",
        "SET TBLPROPERTIES",
        "UNSET TBLPROPERTIES",
    }
)


def equivalent_version(committed: int, history: list[tuple[int, str]]) -> int:
    # Delta refuses time travel past its file retention unless the version is the latest, and
    # predictive optimization keeps adding OPTIMIZE versions that leave the rows unchanged.
    version = committed
    for later, operation in sorted(h for h in history if h[0] > committed):
        if operation not in SAME_DATA_OPERATIONS:
            break
        version = later
    return version


def readable_version(spark, table: str, committed: int) -> int:
    query = (
        f"SELECT version, operation FROM (DESCRIBE HISTORY {table}) "
        f"WHERE version > {int(committed)}"
    )
    history = spark.sql(query).collect()
    return equivalent_version(committed, [(r["version"], r["operation"]) for r in history])

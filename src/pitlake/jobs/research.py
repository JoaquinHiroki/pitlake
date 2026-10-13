import argparse
import uuid
from datetime import UTC, date, datetime, timedelta
from importlib import metadata

from pitlake.backtest import (
    FEATURES,
    GRID,
    STRATEGY,
    UNIVERSE,
    Params,
    Result,
    daily_rows,
    decision_times,
    differences,
    input_fingerprint,
    observations,
    run,
)
from pitlake.bronze import parse_optional_date, sql_string
from pitlake.config import table_fqn, validate_identifier, validate_segment
from pitlake.pit import AS_OF, FeaturesSnapshot, as_of, committed_snapshot, features_version
from pitlake.tables import BACKTEST_RUNS, BACKTEST_RUNS_SCHEMA
from pitlake.tables import FEATURES as FEATURES_TABLE

DEFAULT_START = "2025-02-01"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FEATURES_KEY = f"gold.{FEATURES_TABLE}"
DAILY_COLUMNS = ["at", "pnl", "cost", "net", "gross", "turnover"]


class ReproductionFailed(Exception):
    def __init__(self, failures: dict[str, list[str]], rows: list[tuple]):
        self.failures = failures
        self.rows = rows
        detail = "; ".join(f"{b}: {', '.join(d)}" for b, d in sorted(failures.items())[:10])
        super().__init__(f"{len(failures)} backtests did not reproduce: {detail}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--experiment", required=True, help="MLflow experiment path")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default="", help="Default: the day after the latest close")
    parser.add_argument(
        "--replay", default="", help="A backtest ID or research run ID to reproduce instead"
    )
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.start = parse_optional_date(args.start) or date.fromisoformat(DEFAULT_START)
    args.end = parse_optional_date(args.end)
    args.replay = args.replay.strip()
    if args.replay:
        validate_segment(args.replay)
    if args.end is not None and args.end < args.start:
        parser.error("--end is before --start")
    return args


def package_version() -> str:
    try:
        return metadata.version("pitlake")
    except metadata.PackageNotFoundError:
        return "unknown"


def originals_query(catalog: str, replay: str) -> str:
    key = sql_string(replay)
    return (
        f"SELECT * FROM {table_fqn(catalog, 'control', BACKTEST_RUNS)} "
        f"WHERE replay_of IS NULL AND (backtest_id = {key} OR research_run_id = {key}) "
        "ORDER BY created_at, backtest_id"
    )


def record(
    args,
    params: Params,
    result: Result,
    snapshot: FeaturesSnapshot,
    period: tuple[date, date],
    input_hash: str,
    mlflow_run_id: str,
    at: datetime,
    replay_of: str | None = None,
    reproduced: bool | None = None,
) -> tuple:
    return (
        uuid.uuid4().hex,
        args.run_id,
        at,
        STRATEGY,
        params.to_json(),
        *period,
        {FEATURES_KEY: snapshot.version},
        snapshot.commit_run_id,
        {k: int(v) for k, v in snapshot.inputs.items()},
        args.code_commit,
        package_version(),
        input_hash,
        result.fingerprint,
        dict(result.metrics),
        args.experiment,
        mlflow_run_id,
        replay_of,
        reproduced,
    )


def instant(micros: int) -> datetime:
    return EPOCH + timedelta(microseconds=micros)


def load_inputs(spark, catalog: str, version: int, times: list[datetime]):
    from pyspark.sql import functions as F

    features = features_version(spark, catalog, version).where(F.col("feature").isin(*FEATURES))
    spine = spark.createDataFrame(
        [(a.entity, t) for a in UNIVERSE for t in times], f"entity STRING, {AS_OF} TIMESTAMP"
    )
    rows = (
        as_of(features, spine)
        .select(
            "entity",
            F.unix_micros(AS_OF).alias("at"),
            "feature",
            "value",
            F.unix_micros("event_time").alias("event_time"),
        )
        .collect()
    )
    seen = observations(
        (r["entity"], instant(r["at"]), r["feature"], r["value"], instant(r["event_time"]))
        for r in rows
    )
    return seen, input_fingerprint(seen)


def default_end(spark, catalog: str, version: int) -> date:
    latest = spark.sql(
        "SELECT CAST(to_date(max(event_time)) AS STRING) AS latest "
        f"FROM {table_fqn(catalog, 'gold', FEATURES_TABLE)} VERSION AS OF {int(version)} "
        "WHERE feature = 'close'"
    ).collect()[0]["latest"]
    if latest is None:
        raise RuntimeError("Gold has no close prices to backtest on")
    return date.fromisoformat(latest) + timedelta(days=1)


def log_backtest(mlflow, params, result, tags, period, input_hash) -> str:
    with mlflow.start_run(run_name=params.label, nested=True) as child:
        mlflow.log_params({**{k: str(v) for k, v in vars(params).items()}, "strategy": STRATEGY})
        mlflow.log_params({"period_start": str(period[0]), "period_end": str(period[1])})
        mlflow.set_tags({**tags, "input_hash": input_hash, "result_hash": result.fingerprint})
        mlflow.log_metrics(result.metrics)
        mlflow.log_dict(
            {"columns": DAILY_COLUMNS, "days": daily_rows(result.days)},
            "daily.json",
        )
        return child.info.run_id


def snapshot_tags(args, snapshot: FeaturesSnapshot) -> dict[str, str]:
    return {
        "catalog": args.catalog,
        "research_run_id": args.run_id,
        "gold_features_version": str(snapshot.version),
        "gold_commit_run_id": snapshot.commit_run_id,
        "code_commit": args.code_commit or "unknown",
        "package_version": package_version(),
        **{f"silver.{k}": str(v) for k, v in sorted(snapshot.inputs.items())},
    }


def research(spark, mlflow, args, at: datetime) -> list[tuple]:
    snapshot = committed_snapshot(spark, args.catalog)
    end = args.end or default_end(spark, args.catalog, snapshot.version)
    period = (args.start, end)
    times = decision_times(*period)
    seen, input_hash = load_inputs(spark, args.catalog, snapshot.version, times)
    print(
        f"reading gold.features version {snapshot.version} (Gold commit {snapshot.commit_run_id}): "
        f"{len(seen)} entity-days from {period[0]} to {period[1]}, input {input_hash[:12]}"
    )
    tags = snapshot_tags(args, snapshot)
    rows = []
    with mlflow.start_run(run_name=f"research {args.run_id}"):
        mlflow.set_tags({**tags, "input_hash": input_hash})
        for params in GRID:
            result = run(seen, times, params)
            run_id = log_backtest(mlflow, params, result, tags, period, input_hash)
            rows.append(record(args, params, result, snapshot, period, input_hash, run_id, at))
            m = result.metrics
            print(
                f"{params.label}: return {m['total_return']:+.2%}, sharpe {m['sharpe']:.2f}, "
                f"drawdown {m['max_drawdown']:.2%}, result {result.fingerprint[:12]}"
            )
    return rows


def replay(spark, mlflow, args, at: datetime) -> list[tuple]:
    originals = spark.sql(originals_query(args.catalog, args.replay)).collect()
    if not originals:
        raise RuntimeError(f"no recorded backtest or research run {args.replay!r} to replay")
    inputs = {}
    rows, failures = [], {}
    with mlflow.start_run(run_name=f"replay {args.replay}"):
        mlflow.set_tags({"replay_of": args.replay, "research_run_id": args.run_id})
        for original in originals:
            version = original["table_versions"][FEATURES_KEY]
            period = (original["period_start"], original["period_end"])
            if (version, period) not in inputs:
                inputs[(version, period)] = load_inputs(
                    spark, args.catalog, version, decision_times(*period)
                )
                print(
                    f"read gold.features version {version} again for {period[0]} to {period[1]}, "
                    f"input {inputs[(version, period)][1][:12]}"
                )
            seen, input_hash = inputs[(version, period)]
            params = Params.from_json(original["params"])
            result = run(seen, decision_times(*period), params)
            found = differences(original.asDict(), input_hash, result)
            snapshot = FeaturesSnapshot(
                version, version, original["gold_commit_run_id"], dict(original["silver_versions"])
            )
            tags = {
                **snapshot_tags(args, snapshot),
                "replay_of": original["backtest_id"],
                "reproduced": str(not found).lower(),
            }
            run_id = log_backtest(mlflow, params, result, tags, period, input_hash)
            rows.append(
                record(
                    args,
                    params,
                    result,
                    snapshot,
                    period,
                    input_hash,
                    run_id,
                    at,
                    replay_of=original["backtest_id"],
                    reproduced=not found,
                )
            )
            if original["code_commit"] != args.code_commit:
                print(f"note: recorded with code {original['code_commit']}, now {args.code_commit}")
            state = "IDENTICAL" if not found else "DIFFERENT"
            print(f"{state}: {params.label} ({original['backtest_id']}) {'; '.join(found)}")
            if found:
                failures[original["backtest_id"]] = found
    if failures:
        raise ReproductionFailed(failures, rows)
    return rows


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import mlflow
    from pyspark.sql import SparkSession

    from pitlake.tables import backtest_runs_ddl

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(backtest_runs_ddl(args.catalog))
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(args.experiment)
    at = datetime.now(UTC)

    rows = []
    try:
        rows = replay(spark, mlflow, args, at) if args.replay else research(spark, mlflow, args, at)
    except ReproductionFailed as failed:
        rows = failed.rows
        raise
    finally:
        if rows:
            spark.createDataFrame(rows, schema=BACKTEST_RUNS_SCHEMA).write.mode(
                "append"
            ).saveAsTable(table_fqn(args.catalog, "control", BACKTEST_RUNS))
    kind = "reproduced" if args.replay else "ran"
    print(f"OK: {kind} {len(rows)} backtests in {args.catalog}, logged to {args.experiment}")


if __name__ == "__main__":
    main()

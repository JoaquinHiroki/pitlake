import argparse
import hashlib
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from pitlake.ablation import sample_items
from pitlake.bronze import parse_optional_date
from pitlake.config import table_fqn, validate_identifier, validate_segment, volume_path
from pitlake.harness import HARNESS_REACH, SYMBOLS
from pitlake.information import (
    bar_rows,
    feature_rows,
    filing_rows,
    information_set,
    release_sources,
)
from pitlake.jobs.decide import instant, world
from pitlake.jobs.information_set import SHORT, Loaded, decisions_query, group_decisions, load
from pitlake.jobs.research import package_version
from pitlake.release import (
    RELEASES_VOLUME,
    encode,
    owned_rows,
    payload,
    tool_calls,
    tool_texts,
    unowned,
)
from pitlake.tables import WORLD_BUNDLES, WORLD_BUNDLES_SCHEMA

SAMPLE = 10


class ReleaseFailed(Exception):
    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"{len(failures)} bundles failed: {'; '.join(failures[:5])}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--backtest", default="", help="Backtest or research run; default latest")
    parser.add_argument("--at", default="", help="Decision days (06:00 UTC); default the last")
    parser.add_argument("--check", default="3", help="Items per kind withheld by both engines")
    parser.add_argument("--out", default="", help="Folder for the bundles; default the volume")
    parser.add_argument("--code-commit", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)

    args.catalog = validate_identifier(args.catalog)
    args.run_id = validate_segment(args.run_id.strip() or uuid.uuid4().hex)
    args.backtest = args.backtest.strip()
    if args.backtest:
        validate_segment(args.backtest)
    days = [parse_optional_date(d) for d in re.split(r"[\s,;]+", args.at) if d]
    args.at = tuple(dict.fromkeys(days)) or (None,)
    args.check = int(args.check.strip() or 0)
    if args.check < 0:
        parser.error("--check must not be negative")
    args.out = args.out.strip() or volume_path(args.catalog, "control", RELEASES_VOLUME)
    return args


@dataclass(frozen=True)
class Released:
    name: str
    bundle_id: str
    at: datetime
    record: dict
    versions: dict
    set_hash: str
    items: dict[str, int]
    rows: int
    path: str
    sha256: str
    size: int
    engine: str
    checked: tuple[str, ...]
    problems: tuple[str, ...]
    seconds: float

    @property
    def verified(self) -> bool:
        return not self.problems


def compare_tools(engine_world, rows: list[tuple], at: datetime) -> list[str]:
    from pitlake_counterfactual.agent import Session

    session = Session.of(engine_world)
    theirs = tool_texts(rows, at)
    problems = []
    for (name, arguments), key in zip(tool_calls(), theirs, strict=True):
        if session.answer(name, arguments) != theirs[key]:
            problems.append(f"{key} reads differently from the engine")
    return problems


def check_bundle(path: Path, loaded: Loaded, info, at: datetime, per_kind: int):
    from pitlake_counterfactual.world import Bundle, differences

    bundle = Bundle.read(path)
    problems = [f"engine: {p}" for p in bundle.check()]
    if bundle.set_hash != info.hash:
        problems.append(f"the engine's set {bundle.set_hash[:SHORT]} is not {info.hash[:SHORT]}")
    problems += [f"null world: {p}" for p in compare_tools(bundle.withhold(()), loaded.rows, at)]
    checked = tuple(sample_items(info.items.values(), frozenset(info.items), per_kind))
    for item in checked:
        theirs = world(loaded, (item,))
        mine = bundle.withhold([item])
        problems += [f"without {item}: {p}" for p in mine.problems]
        problems += [
            f"without {item}: {p}" for p in differences(mine.rows, owned_rows(theirs, at))[:SAMPLE]
        ]
        problems += [f"without {item}: {p}" for p in compare_tools(mine, theirs, at)]
    return bundle, checked, problems


def release_one(loaded: Loaded, record: dict, day: date | None, out: str, per_kind: int, args):
    from pyspark.sql import functions as F

    from pitlake_counterfactual import __version__ as engine

    started = time.perf_counter()
    at, _ = instant(record, day)
    info = information_set(loaded.items, at)
    symbols = list(SYMBOLS)
    bars = bar_rows(loaded.bars.where(F.col("symbol").isin(*symbols)))
    filings = filing_rows(loaded.fundamentals.where(F.col("symbol").isin(*symbols)))
    releases = feature_rows(release_sources(loaded.features, HARNESS_REACH))
    source = {
        "catalog": args.catalog,
        "backtest_id": record["backtest_id"],
        "research_run_id": record["research_run_id"],
        "table_versions": dict(loaded.versions),
        "code_commit": args.code_commit,
        "package_version": package_version(),
    }
    found = payload(at, info.items, bars, filings, releases, loaded.rows, info.hash, source)
    data = encode(found)
    path = Path(out) / f"{found['name']}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    problems = [f"PITLake: {p}" for p in unowned(loaded.rows, info.items, at)[:SAMPLE]]
    bundle, checked, more = check_bundle(path, loaded, info, at, per_kind)
    return Released(
        found["name"],
        found["id"],
        at,
        record,
        dict(loaded.versions),
        info.hash,
        bundle.by_kind(),
        len(found["rows"]),
        str(path),
        hashlib.sha256(data).hexdigest(),
        len(data),
        engine,
        checked,
        tuple(problems + more),
        time.perf_counter() - started,
    )


def bundle_row(args, r: Released, created: datetime) -> tuple:
    return (
        r.bundle_id,
        r.name,
        args.run_id,
        created,
        r.record["backtest_id"],
        r.record["research_run_id"],
        r.at,
        r.versions,
        r.set_hash,
        r.items,
        r.rows,
        r.path,
        r.sha256,
        r.size,
        r.engine,
        list(r.checked),
        list(r.problems),
        r.verified,
        r.seconds,
        args.code_commit,
        package_version(),
    )


def describe(r: Released) -> str:
    kinds = ", ".join(f"{k} {n:,}" for k, n in r.items.items())
    head = (
        f"{r.name} at {r.at:%Y-%m-%d %H:%M} UTC: set {r.set_hash[:SHORT]}, "
        f"{sum(r.items.values()):,} items ({kinds}), {r.rows:,} rows, "
        f"{r.size / 1e6:.1f} MB, sha256 {r.sha256[:SHORT]}, id {r.bundle_id[:SHORT]}"
    )
    if r.verified:
        lines = [
            f"RELEASED: {head}; engine {r.engine} verifies it and withholds "
            f"{len(r.checked)} items as PITLake does ({' '.join(r.checked) or 'none'})"
        ]
    else:
        lines = [f"FAILED: {head}; {len(r.problems)} problems"]
        lines += [f"  {p}" for p in r.problems[: SAMPLE * 2]]
    lines.append(f"  {r.path}")
    return "\n".join(lines)


def release_all(spark, args) -> list[Released]:
    records = spark.sql(decisions_query(args.catalog, args.backtest)).collect()
    if not records:
        raise RuntimeError(f"no recorded backtest {args.backtest or '(latest run)'}")
    record = records[0].asDict()
    key = next(iter(group_decisions([record])))
    loaded = load(spark, args.catalog, key, HARNESS_REACH)
    found = []
    for day in args.at:
        released = release_one(loaded, record, day, args.out, args.check, args)
        print(describe(released))
        found.append(released)
    return found


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    from pyspark.sql import SparkSession

    from pitlake.tables import counterfactual_ddls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    for ddl in counterfactual_ddls(args.catalog):
        spark.sql(ddl)

    created = datetime.now(UTC)
    found = release_all(spark, args)
    rows = [bundle_row(args, r, created) for r in found]
    spark.createDataFrame(rows, schema=WORLD_BUNDLES_SCHEMA).write.mode("append").saveAsTable(
        table_fqn(args.catalog, "control", WORLD_BUNDLES)
    )
    failures = [r.name for r in found if not r.verified]
    if failures:
        raise ReleaseFailed(failures)
    print(f"OK: {len(found)} bundles released from {args.catalog}; download them with")
    for r in found:
        print(f"  databricks fs cp dbfs:{r.path} .")


if __name__ == "__main__":
    main()

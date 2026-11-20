import argparse
import hashlib
import re
import time
import uuid
from collections.abc import Callable
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
from pitlake.jobs.suffice import Builder
from pitlake.jobs.value import venues_of
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
from pitlake.value import FEED, sources

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
    parser.add_argument(
        "--without", default="", help="Feeds withheld whole from a second bundle, e.g. alpaca"
    )
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
    args.without = tuple(sorted(dict.fromkeys(f for f in re.split(r"[\s,;]+", args.without) if f)))
    for feed in args.without:
        validate_segment(feed)
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
    licence: str = ""
    without: tuple[str, ...] = ()
    withheld: int = 0
    full: str = ""

    @property
    def verified(self) -> bool:
        return not self.problems


Rows = Callable[[tuple[str, ...]], list[tuple]]


def compare_tools(engine_world, rows: list[tuple], at: datetime) -> list[str]:
    from pitlake_counterfactual.agent import Session

    session = Session.of(engine_world)
    theirs = tool_texts(rows, at)
    problems = []
    for (name, arguments), key in zip(tool_calls(), theirs, strict=True):
        if session.answer(name, arguments) != theirs[key]:
            problems.append(f"{key} reads differently from the engine")
    return problems


def check_bundle(path: Path, rows_without: Rows, info, at: datetime, per_kind: int):
    from pitlake_counterfactual.world import Bundle, differences

    bundle = Bundle.read(path)
    problems = [f"engine: {p}" for p in bundle.check()]
    if bundle.set_hash != info.hash:
        problems.append(f"the engine's set {bundle.set_hash[:SHORT]} is not {info.hash[:SHORT]}")
    null = compare_tools(bundle.withhold(()), rows_without(()), at)
    problems += [f"null world: {p}" for p in null]
    checked = tuple(sample_items(info.items.values(), frozenset(info.items), per_kind))
    for item in checked:
        theirs = rows_without((item,))
        mine = bundle.withhold([item])
        problems += [f"without {item}: {p}" for p in mine.problems]
        problems += [
            f"without {item}: {p}" for p in differences(mine.rows, owned_rows(theirs, at))[:SAMPLE]
        ]
        problems += [f"without {item}: {p}" for p in compare_tools(mine, theirs, at)]
    return bundle, checked, problems


def check_against_full(bundle, full_path: str, withheld: frozenset[str]) -> list[str]:
    from pitlake_counterfactual.world import Bundle, differences

    full = Bundle.read(full_path)
    mine = full.withhold([i for i in full.items if i in withheld])
    problems = [f"from the full bundle: {p}" for p in mine.problems]
    problems += [f"from the full bundle: {p}" for p in differences(mine.rows, bundle.rows)[:SAMPLE]]
    if mine.hash != bundle.withhold(()).hash:
        problems.append("the full bundle without the feeds hashes differently from this bundle")
    return problems


@dataclass(frozen=True)
class Inputs:
    at: datetime
    bars: list[tuple]
    filings: list[tuple]
    releases: list[tuple]
    source: dict


def inputs_of(loaded: Loaded, record: dict, day: date | None, args) -> Inputs:
    from pyspark.sql import functions as F

    at, _ = instant(record, day)
    symbols = list(SYMBOLS)
    return Inputs(
        at,
        bar_rows(loaded.bars.where(F.col("symbol").isin(*symbols))),
        filing_rows(loaded.fundamentals.where(F.col("symbol").isin(*symbols))),
        feature_rows(release_sources(loaded.features, HARNESS_REACH)),
        {
            "catalog": args.catalog,
            "backtest_id": record["backtest_id"],
            "research_run_id": record["research_run_id"],
            "table_versions": dict(loaded.versions),
            "code_commit": args.code_commit,
            "package_version": package_version(),
        },
    )


def export(
    loaded: Loaded,
    record: dict,
    given: Inputs,
    info,
    rows: list[tuple],
    rows_without: Rows,
    out: str,
    per_kind: int,
    started: float,
    without: tuple[str, ...] = (),
    full: Released | None = None,
) -> Released:
    from pitlake_counterfactual import __version__ as engine

    at = given.at
    source = dict(given.source)
    if without:
        source.update(without=list(without), full_set_hash=full.set_hash, full_bundle=full.name)
    found = payload(
        at, info.items, given.bars, given.filings, given.releases, rows, info.hash, source, without
    )
    data = encode(found)
    path = Path(out) / f"{found['name']}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    problems = [f"PITLake: {p}" for p in unowned(rows, info.items, at)[:SAMPLE]]
    bundle, checked, more = check_bundle(path, rows_without, info, at, per_kind)
    if without:
        withheld = frozenset(full_items(loaded, at)) - frozenset(info.items)
        more += check_against_full(bundle, full.path, withheld)
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
        found["terms"]["licence"],
        without,
        sum(full.items.values()) - sum(bundle.by_kind().values()) if full else 0,
        full.name if full else "",
    )


def full_items(loaded: Loaded, at: datetime) -> dict:
    return information_set(loaded.items, at).items


def release_one(loaded: Loaded, record: dict, day: date | None, out: str, per_kind: int, args):
    started = time.perf_counter()
    given = inputs_of(loaded, record, day, args)
    info = information_set(loaded.items, given.at)

    def rows_without(extra: tuple[str, ...]) -> list[tuple]:
        return world(loaded, extra)

    return export(loaded, record, given, info, loaded.rows, rows_without, out, per_kind, started)


def feed_items(loaded: Loaded, feeds: tuple[str, ...]) -> frozenset[str]:
    found = {s.name: s.items for s in sources(loaded.items, venues_of(loaded.bars), (FEED,))}
    unknown = [f for f in feeds if f not in found]
    if unknown:
        raise ValueError(f"no feed {unknown} in the information set; feeds are {sorted(found)}")
    return frozenset().union(*(found[f] for f in feeds))


def release_without(
    spark,
    builder: Builder,
    full: Released,
    record: dict,
    day: date | None,
    out: str,
    per_kind: int,
    args,
) -> Released:
    started = time.perf_counter()
    loaded = builder.loaded
    given = inputs_of(loaded, record, day, args)
    dropped = feed_items(loaded, args.without)
    kept = frozenset(i for i in builder.known if i not in dropped)
    info = information_set([i for i in loaded.items if i.id not in dropped], given.at)

    def rows_without(extra: tuple[str, ...]) -> list[tuple]:
        return builder.rows(builder.build(spark, kept - set(extra)), HARNESS_REACH)

    return export(
        loaded,
        record,
        given,
        info,
        rows_without(()),
        rows_without,
        out,
        per_kind,
        started,
        args.without,
        full,
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
        f"{r.size / 1e6:.1f} MB, sha256 {r.sha256[:SHORT]}, id {r.bundle_id[:SHORT]}, "
        f"licence {r.licence or 'unknown'}"
    )
    if r.verified:
        found = (
            f"RELEASED: {head}; engine {r.engine} verifies it and withholds "
            f"{len(r.checked)} items as PITLake does ({' '.join(r.checked) or 'none'})"
        )
        if r.without:
            found += (
                f", and is {r.full} without {' '.join(r.without)} ({r.withheld:,} items) "
                "as the engine withholds them"
            )
        lines = [found]
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
    builder = Builder(loaded) if args.without else None
    found = []
    for day in args.at:
        full = release_one(loaded, record, day, args.out, args.check, args)
        print(describe(full))
        found.append(full)
        if builder and full.verified:
            public = release_without(spark, builder, full, record, day, args.out, args.check, args)
            print(describe(public))
            found.append(public)
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
        print(f"  databricks fs cp dbfs:{r.path} .    # {r.licence}")


if __name__ == "__main__":
    main()

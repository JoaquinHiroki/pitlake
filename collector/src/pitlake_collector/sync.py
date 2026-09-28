"""One collection cycle: find what is missing from the landing volume and fetch it.

The landing volume is the only state. A partition is done when its manifest exists there,
so backfill, daily catch-up and recovery after downtime are the same operation.
"""

import logging
import socket
import threading
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pitlake.config import MANIFEST_SUFFIX, manifest_name
from pitlake.manifest import LandingManifest
from pitlake_collector import __version__
from pitlake_collector.config import CollectorConfig, Feed
from pitlake_collector.landing import LandingStore
from pitlake_collector.sources import Http, build_source
from pitlake_collector.sources.base import (
    ChecksumMismatch,
    FetchedFile,
    NotPublished,
    Partition,
    Source,
)

log = logging.getLogger("pitlake_collector")

# How long after its expected publication a missing file is still "not yet published"
# rather than a gap worth a warning.
_PUBLICATION_GRACE = timedelta(days=2)
_CHECKSUM_ATTEMPTS = 2


@dataclass
class FeedResult:
    feed: str
    symbol: str
    missing: int = 0
    attempted: int = 0
    landed: int = 0
    not_published: int = 0
    failed: list[str] = field(default_factory=list)


def plan(source: Source, symbol: str, start: date, end: date, landed: set[str]) -> list[Partition]:
    """Partitions in [start, end] whose manifest has not landed, oldest first."""
    return [
        p for p in source.partitions(symbol, start, end) if manifest_name(p.file_name) not in landed
    ]


def _fetch_verified(source: Source, partition: Partition, dest_dir: Path) -> FetchedFile:
    for attempt in range(1, _CHECKSUM_ATTEMPTS + 1):
        try:
            return source.fetch(partition, dest_dir)
        except ChecksumMismatch:
            if attempt == _CHECKSUM_ATTEMPTS:
                raise
            log.warning("checksum mismatch, retrying", extra={"file": partition.file_name})
    raise AssertionError("unreachable")


def _manifest(fetched: FetchedFile) -> LandingManifest:
    p = fetched.partition
    return LandingManifest(
        source=p.source,
        dataset=p.dataset,
        symbol=p.symbol,
        period_start=p.period_start,
        period_end=p.period_end,
        file_name=p.file_name,
        landing_path=p.relpath,
        source_url=p.url,
        sha256=fetched.sha256,
        size_bytes=fetched.size_bytes,
        fetched_at=fetched.fetched_at,
        collector_version=__version__,
        collector_host=socket.gethostname(),
    )


def sync_symbol(
    source: Source,
    symbol: str,
    start: date,
    end: date,
    landing: LandingStore,
    download_dir: Path,
    max_files: int,
    stop: threading.Event | None = None,
) -> FeedResult:
    result = FeedResult(feed=f"{source.name}.{source.dataset}", symbol=symbol)
    todo = plan(source, symbol, start, end, landing.list_names(source.landing_dir(symbol)))
    result.missing = len(todo)
    grace_start = end - _PUBLICATION_GRACE

    for partition in todo[:max_files] if max_files else todo:
        if stop is not None and stop.is_set():
            break
        result.attempted += 1
        context = {"feed": result.feed, "symbol": symbol, "file": partition.file_name}
        try:
            fetched = _fetch_verified(source, partition, download_dir)
        except NotPublished:
            result.not_published += 1
            if partition.period_end >= grace_start:
                log.info("not yet published", extra=context)
            else:
                log.warning("missing upstream", extra={**context, "url": partition.url})
            continue
        except Exception:
            log.exception("fetch failed", extra=context)
            result.failed.append(partition.file_name)
            continue

        try:
            landing.put_file(partition.relpath, fetched.local_path)
            landing.put_text(partition.relpath + MANIFEST_SUFFIX, _manifest(fetched).to_json())
        except Exception:
            log.exception("upload failed", extra=context)
            result.failed.append(partition.file_name)
            continue
        finally:
            fetched.local_path.unlink(missing_ok=True)

        result.landed += 1
        log.info(
            "landed",
            extra={**context, "sha256": fetched.sha256, "size_bytes": fetched.size_bytes},
        )
    return result


def feed_window(
    feed: Feed, source: Source, today: date, start: date | None, end: date | None
) -> tuple[date, date]:
    latest = source.latest_available(today)
    window_start = max(start or feed.start_date, feed.start_date)
    window_end = min(end or feed.end_date or latest, feed.end_date or latest, latest)
    return window_start, window_end


def run_cycle(
    config: CollectorConfig,
    landing: LandingStore,
    http: Http,
    *,
    start: date | None = None,
    end: date | None = None,
    max_files: int | None = None,
    stop: threading.Event | None = None,
    today: date | None = None,
) -> list[FeedResult]:
    download_dir = config.work_dir / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    for leftover in download_dir.glob("*"):
        leftover.unlink(missing_ok=True)

    today = today or datetime.now(UTC).date()
    limit = config.max_files_per_cycle if max_files is None else max_files
    results = []
    for feed in config.feeds:
        source = build_source(feed.source, feed.dataset, http)
        window_start, window_end = feed_window(feed, source, today, start, end)
        for symbol in feed.symbols:
            source.validate_symbol(symbol)
            if window_end < window_start:
                continue
            result = sync_symbol(
                source, symbol, window_start, window_end, landing, download_dir, limit, stop
            )
            results.append(result)
            log.info(
                "feed synced",
                extra={
                    "feed": result.feed,
                    "symbol": symbol,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "missing": result.missing,
                    "attempted": result.attempted,
                    "landed": result.landed,
                    "not_published": result.not_published,
                    "failed": len(result.failed),
                },
            )
    return results

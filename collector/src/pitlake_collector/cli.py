"""Command line entry point.

  pitlake-collector sync --config collector.toml      one cycle, then exit (Databricks job, cron)
  pitlake-collector run  --config collector.toml      cycle forever (systemd on a VM)

--feed source.dataset limits either command to one feed of the config; the ingest job runs one
collect iteration per registered dataset this way (ADR 0005).

Where files land:
  --landing volume     the /Volumes mount; use when running on Databricks
  --landing api        the Files API (default); needs DATABRICKS_HOST and DATABRICKS_TOKEN or
                       DATABRICKS_CONFIG_PROFILE in the environment, never in the config file
  --landing-dir PATH   a local directory, for trying the collector without a workspace
"""

import argparse
import logging
import signal
import tempfile
import threading
from dataclasses import replace
from datetime import date
from pathlib import Path

from pitlake_collector import __version__
from pitlake_collector.config import CollectorConfig, load_config
from pitlake_collector.http import HttpClient
from pitlake_collector.landing import (
    DatabricksLanding,
    LandingStore,
    LocalLanding,
    VolumeLanding,
)
from pitlake_collector.logs import setup_logging
from pitlake_collector.secrets import secret_reader
from pitlake_collector.sync import run_cycle

log = logging.getLogger("pitlake_collector")


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", type=Path, required=True)
    common.add_argument("--landing", choices=("api", "volume"), default="api")
    common.add_argument("--landing-dir", type=Path, help="Write to this local directory instead")
    common.add_argument("--work-dir", type=Path, help="Override work_dir from the config")
    common.add_argument(
        "--private-work-dir",
        action="store_true",
        help="Download into a new private temporary directory, removed on exit. For parallel job "
        "tasks, which can share a machine under different users (ADR 0005).",
    )
    common.add_argument("--log-level", default="INFO")
    common.add_argument("--feed", help="Only this feed, e.g. alpaca.stock_trades")

    parser = argparse.ArgumentParser(prog="pitlake-collector")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    sync = commands.add_parser("sync", parents=[common], help="Run one cycle and exit")
    sync.add_argument("--start", type=date.fromisoformat, help="Only fetch from this date")
    sync.add_argument("--end", type=date.fromisoformat, help="Only fetch up to this date")
    sync.add_argument("--max-files", type=int, help="Per symbol; 0 means no limit")

    commands.add_parser("run", parents=[common], help="Run cycles until stopped")
    return parser


def build_landing(config: CollectorConfig, mode: str, landing_dir: Path | None) -> LandingStore:
    if landing_dir is not None:
        return LocalLanding(landing_dir)
    if mode == "volume":
        return VolumeLanding(config.catalog)
    from databricks.sdk import WorkspaceClient

    return DatabricksLanding(WorkspaceClient().files, config.catalog)


def select_feed(config: CollectorConfig, feed: str | None) -> CollectorConfig:
    """The config limited to one feed. A registered dataset this config does not collect (none
    configured for this target) leaves no feeds, which is not an error."""
    if feed is None:
        return config
    return replace(
        config, feeds=tuple(f for f in config.feeds if f"{f.source}.{f.dataset}" == feed)
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    setup_logging(args.log_level)
    config = select_feed(load_config(args.config), args.feed)
    if not config.feeds:
        log.info("no feed configured", extra={"feed": args.feed})
        return 0
    if args.private_work_dir:
        with tempfile.TemporaryDirectory(prefix="pitlake-collector-") as work_dir:
            return _run(args, replace(config, work_dir=Path(work_dir)))
    if args.work_dir is not None:
        config = replace(config, work_dir=args.work_dir)
    return _run(args, config)


def _run(args: argparse.Namespace, config: CollectorConfig) -> int:
    landing = build_landing(config, args.landing, args.landing_dir)
    http = HttpClient(
        user_agent=f"pitlake-collector/{__version__}",
        min_interval_seconds=config.request_interval_seconds,
    )
    secrets = secret_reader(config.secret_scope)
    log.info(
        "starting",
        extra={"command": args.command, "landing": landing.describe(), "version": __version__},
    )

    if args.command == "sync":
        results = run_cycle(
            config,
            landing,
            http,
            start=args.start,
            end=args.end,
            max_files=args.max_files,
            secrets=secrets,
        )
        return 1 if any(r.failed for r in results) else 0

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        try:
            run_cycle(config, landing, http, stop=stop, secrets=secrets)
        except Exception:
            log.exception("cycle failed")
        stop.wait(config.interval_minutes * 60)
    log.info("stopped")
    return 0


def entry() -> None:
    """Console-script entry point.

    Databricks wheel tasks call the entry point and ignore its return value, so a failure has to
    be raised, not returned, for the task to fail.
    """
    code = main()
    if code:
        raise SystemExit(f"pitlake-collector exited with status {code}; see the log above")

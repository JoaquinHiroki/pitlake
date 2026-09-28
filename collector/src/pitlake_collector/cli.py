"""Command line entry point.

  pitlake-collector sync --config collector.toml      one cycle, then exit (backfills, cron)
  pitlake-collector run  --config collector.toml      cycle forever (systemd)

Databricks credentials come from the environment (DATABRICKS_HOST and DATABRICKS_TOKEN, or
DATABRICKS_CONFIG_PROFILE), never from the config file.
"""

import argparse
import logging
import signal
import threading
from dataclasses import replace
from datetime import date
from pathlib import Path

from pitlake_collector import __version__
from pitlake_collector.config import CollectorConfig, load_config
from pitlake_collector.http import HttpClient
from pitlake_collector.landing import DatabricksLanding, LandingStore, LocalLanding
from pitlake_collector.logs import setup_logging
from pitlake_collector.sync import run_cycle

log = logging.getLogger("pitlake_collector")


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", type=Path, required=True)
    common.add_argument(
        "--landing-dir",
        type=Path,
        help="Write to this local directory instead of the Databricks volume",
    )
    common.add_argument("--work-dir", type=Path, help="Override work_dir from the config")
    common.add_argument("--log-level", default="INFO")

    parser = argparse.ArgumentParser(prog="pitlake-collector")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    sync = commands.add_parser("sync", parents=[common], help="Run one cycle and exit")
    sync.add_argument("--start", type=date.fromisoformat, help="Only fetch from this date")
    sync.add_argument("--end", type=date.fromisoformat, help="Only fetch up to this date")
    sync.add_argument("--max-files", type=int, help="Per symbol; 0 means no limit")

    commands.add_parser("run", parents=[common], help="Run cycles until stopped")
    return parser


def _landing(config: CollectorConfig, landing_dir: Path | None) -> LandingStore:
    if landing_dir is not None:
        return LocalLanding(landing_dir)
    from databricks.sdk import WorkspaceClient

    return DatabricksLanding(WorkspaceClient().files, config.catalog)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    setup_logging(args.log_level)
    config = load_config(args.config)
    if args.work_dir is not None:
        config = replace(config, work_dir=args.work_dir)
    landing = _landing(config, args.landing_dir)
    http = HttpClient(
        user_agent=f"pitlake-collector/{__version__}",
        min_interval_seconds=config.request_interval_seconds,
    )
    log.info(
        "starting",
        extra={"command": args.command, "landing": landing.describe(), "version": __version__},
    )

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    if args.command == "sync":
        results = run_cycle(
            config,
            landing,
            http,
            start=args.start,
            end=args.end,
            max_files=args.max_files,
            stop=stop,
        )
        return 1 if any(r.failed for r in results) else 0

    while not stop.is_set():
        try:
            run_cycle(config, landing, http, stop=stop)
        except Exception:
            log.exception("cycle failed")
        stop.wait(config.interval_minutes * 60)
    log.info("stopped")
    return 0

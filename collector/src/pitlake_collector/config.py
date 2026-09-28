"""Collector configuration, read from a TOML file. See config.example.toml."""

import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pitlake.config import validate_identifier


@dataclass(frozen=True)
class Feed:
    source: str
    dataset: str
    symbols: tuple[str, ...]
    start_date: date
    # None means "keep up with the latest file the source has published".
    end_date: date | None = None


@dataclass(frozen=True)
class CollectorConfig:
    catalog: str
    work_dir: Path
    feeds: tuple[Feed, ...]
    interval_minutes: int = 60
    max_files_per_cycle: int = 50
    request_interval_seconds: float = 0.5


def _as_date(value: object, field: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"{field}: expected a date, got {value!r}")


def parse_config(data: dict) -> CollectorConfig:
    feeds = []
    for i, raw in enumerate(data.get("feeds", [])):
        symbols = tuple(raw.get("symbols", ()))
        if not symbols:
            raise ValueError(f"feeds[{i}]: at least one symbol is required")
        start = _as_date(raw["start_date"], f"feeds[{i}].start_date")
        end = _as_date(raw["end_date"], f"feeds[{i}].end_date") if "end_date" in raw else None
        if end is not None and end < start:
            raise ValueError(f"feeds[{i}]: end_date is before start_date")
        feeds.append(
            Feed(
                source=validate_identifier(raw["source"]),
                dataset=validate_identifier(raw["dataset"]),
                symbols=symbols,
                start_date=start,
                end_date=end,
            )
        )
    if not feeds:
        raise ValueError("config has no feeds")

    config = CollectorConfig(
        catalog=validate_identifier(data["catalog"]),
        work_dir=Path(data.get("work_dir", "/var/lib/pitlake-collector")),
        feeds=tuple(feeds),
        interval_minutes=int(data.get("interval_minutes", 60)),
        max_files_per_cycle=int(data.get("max_files_per_cycle", 50)),
        request_interval_seconds=float(data.get("request_interval_seconds", 0.5)),
    )
    if config.interval_minutes < 1:
        raise ValueError("interval_minutes must be at least 1")
    if config.max_files_per_cycle < 0:
        raise ValueError("max_files_per_cycle must be 0 (no limit) or positive")
    return config


def load_config(path: Path) -> CollectorConfig:
    with open(path, "rb") as fh:
        return parse_config(tomllib.load(fh))

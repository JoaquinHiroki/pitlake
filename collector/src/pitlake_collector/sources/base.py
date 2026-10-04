"""The interface every source implements.

A source knows how to name the files it publishes (partitions) and how to fetch one and
prove it arrived intact. It knows nothing about Databricks; landing is someone else's job.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Protocol

from pitlake.config import landing_dir, landing_relpath


class Http(Protocol):
    def get_text(self, url: str) -> str: ...
    def get_bytes(self, url: str) -> bytes: ...
    def download(self, url: str, dest: Path) -> tuple[str, int]: ...


# Reads a credential by name, e.g. "fred-api-key". See pitlake_collector.secrets.
Secrets = Callable[[str], str]


class NotPublished(Exception):
    """The source has no file for this partition, yet or at all."""


class ChecksumMismatch(Exception):
    """The downloaded bytes do not match what the source says they should be."""


class InvalidResponse(Exception):
    """An API answered, but not with what the partition should contain. Nothing is landed."""


@dataclass(frozen=True)
class Partition:
    source: str
    dataset: str
    symbol: str
    period_start: date
    period_end: date
    file_name: str
    url: str

    @property
    def relpath(self) -> str:
        return landing_relpath(self.source, self.dataset, self.symbol, self.file_name)


@dataclass(frozen=True)
class FetchedFile:
    partition: Partition
    local_path: Path
    sha256: str
    size_bytes: int
    fetched_at: datetime


class Source(ABC):
    name: ClassVar[str]
    dataset: ClassVar[str]

    @classmethod
    def create(cls, http: Http, secrets: Secrets) -> "Source":
        """Build the source. Sources that need credentials override this to ask for them."""
        return cls(http)

    def landing_dir(self, symbol: str) -> str:
        return landing_dir(self.name, self.dataset, symbol)

    @abstractmethod
    def validate_symbol(self, symbol: str) -> str: ...

    @abstractmethod
    def latest_available(self, today: date) -> date:
        """Last period the source is expected to have published by `today` (UTC)."""

    @abstractmethod
    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        """Every file covering [start, end], oldest first. May ask the source what exists."""

    @abstractmethod
    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        """Download and verify one partition. Raises NotPublished or ChecksumMismatch."""

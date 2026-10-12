from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Protocol

from pitlake.config import landing_dir, landing_relpath


class Http(Protocol):
    def get_text(self, url: str) -> str: ...
    def get_bytes(self, url: str, headers: dict[str, str] | None = None) -> bytes: ...
    def download(self, url: str, dest: Path) -> tuple[str, int]: ...


Secrets = Callable[[str], str]


class NotPublished(Exception):
    pass


class ChecksumMismatch(Exception):
    pass


class InvalidResponse(Exception):
    pass


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
        return cls(http)

    def landing_dir(self, symbol: str) -> str:
        return landing_dir(self.name, self.dataset, symbol)

    @abstractmethod
    def validate_symbol(self, symbol: str) -> str: ...

    @abstractmethod
    def latest_available(self, today: date) -> date:
        pass

    @abstractmethod
    def partitions(self, symbol: str, start: date, end: date) -> list[Partition]:
        pass

    @abstractmethod
    def fetch(self, partition: Partition, dest_dir: Path) -> FetchedFile:
        pass

import hashlib
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk.errors import NotFound as SdkNotFound

from pitlake_collector.http import NotFound


class FakeHttp:
    """Serves files from a dict; a CHECKSUM entry is derived unless overridden."""

    def __init__(self, files: dict[str, bytes], checksums: dict[str, str] | None = None):
        self.files = files
        self.checksums = checksums or {}
        self.requests: list[str] = []
        self.headers: list[dict[str, str]] = []

    def get_text(self, url: str) -> str:
        self.requests.append(url)
        data_url = url.removesuffix(".CHECKSUM")
        if data_url not in self.files:
            raise NotFound(url)
        name = data_url.rsplit("/", 1)[1]
        digest = self.checksums.get(data_url, hashlib.sha256(self.files[data_url]).hexdigest())
        return f"{digest}  {name}\n"

    def get_bytes(self, url: str, headers: dict[str, str] | None = None) -> bytes:
        self.requests.append(url)
        self.headers.append(headers or {})
        if url not in self.files:
            raise NotFound(url)
        return self.files[url]

    def download(self, url: str, dest: Path) -> tuple[str, int]:
        self.requests.append(url)
        if url not in self.files:
            raise NotFound(url)
        dest.write_bytes(self.files[url])
        return hashlib.sha256(self.files[url]).hexdigest(), len(self.files[url])


@dataclass
class Entry:
    name: str
    is_directory: bool


@dataclass
class Metadata:
    content_length: int


class FakeFilesApi:
    """The subset of WorkspaceClient().files the collector uses."""

    def __init__(self, truncate: bool = False):
        self.store: dict[str, bytes] = {}
        self.directories: set[str] = set()
        self.truncate = truncate

    def create_directory(self, path: str) -> None:
        assert path.endswith("/")
        self.directories.add(path)

    def upload(self, path: str, contents, overwrite: bool = False) -> None:
        data = contents.read()
        self.store[path] = data[:-1] if self.truncate and data else data

    def get_metadata(self, path: str) -> Metadata:
        return Metadata(content_length=len(self.store[path]))

    def list_directory_contents(self, path: str):
        if not any(k.startswith(path) for k in self.store):
            raise SdkNotFound("no such directory")
        return [
            Entry(name=k[len(path) :], is_directory=False)
            for k in self.store
            if k.startswith(path) and "/" not in k[len(path) :]
        ]

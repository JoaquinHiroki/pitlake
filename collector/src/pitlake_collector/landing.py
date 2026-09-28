"""Where fetched files go: the Unity Catalog landing volume, or a local directory for dry runs.

On Databricks the collector writes through the /Volumes mount (VolumeLanding); anywhere else it
uses the Files API (DatabricksLanding). Both produce the same layout.

Upload order is the atomicity guarantee. The data file goes first and its size is checked;
the manifest goes last. The platform only loads files that have a manifest, so a transfer
that dies halfway is invisible to it and is simply redone on the next cycle.
"""

import io
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from pitlake.config import landing_path


class UploadError(Exception):
    pass


class LandingStore(Protocol):
    def list_names(self, reldir: str) -> set[str]: ...
    def put_file(self, relpath: str, local_path: Path) -> None: ...
    def put_text(self, relpath: str, text: str) -> None: ...
    def describe(self) -> str: ...


class DatabricksLanding:
    """Writes through the Files API, so the VM needs no Spark and no cluster."""

    def __init__(self, files_api, catalog: str) -> None:
        self._files = files_api
        self._root = landing_path(catalog)
        self._known_dirs: set[str] = set()

    def describe(self) -> str:
        return self._root

    def _abs(self, relpath: str) -> str:
        return f"{self._root}/{relpath}"

    def _ensure_dir(self, relpath: str) -> None:
        parent = relpath.rpartition("/")[0]
        if parent and parent not in self._known_dirs:
            self._files.create_directory(self._abs(parent) + "/")
            self._known_dirs.add(parent)

    def list_names(self, reldir: str) -> set[str]:
        from databricks.sdk.errors import NotFound

        try:
            entries = self._files.list_directory_contents(self._abs(reldir) + "/")
            return {e.name for e in entries if not e.is_directory}
        except NotFound:
            return set()

    def put_file(self, relpath: str, local_path: Path) -> None:
        self._ensure_dir(relpath)
        size = local_path.stat().st_size
        with open(local_path, "rb") as fh:
            self._files.upload(self._abs(relpath), fh, overwrite=True)
        landed = self._files.get_metadata(self._abs(relpath)).content_length
        if landed != size:
            raise UploadError(f"{relpath}: uploaded {landed} bytes, expected {size}")

    def put_text(self, relpath: str, text: str) -> None:
        self._ensure_dir(relpath)
        self._files.upload(self._abs(relpath), io.BytesIO(text.encode()), overwrite=True)


class LocalLanding:
    """Same layout on a local filesystem. Used for trying the collector without a workspace."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def describe(self) -> str:
        return str(self._root)

    def list_names(self, reldir: str) -> set[str]:
        directory = self._root / reldir
        if not directory.is_dir():
            return set()
        return {p.name for p in directory.iterdir() if p.is_file()}

    def _target(self, relpath: str) -> Path:
        target = self._root / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def _place(self, target: Path, write: Callable[[Path], None]) -> None:
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        os.close(fd)
        try:
            write(Path(tmp))
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def put_file(self, relpath: str, local_path: Path) -> None:
        target = self._target(relpath)
        self._place(target, lambda dst: shutil.copyfile(local_path, dst))
        landed, size = target.stat().st_size, local_path.stat().st_size
        if landed != size:
            raise UploadError(f"{relpath}: wrote {landed} bytes, expected {size}")

    def put_text(self, relpath: str, text: str) -> None:
        self._place(self._target(relpath), lambda dst: dst.write_bytes(text.encode()))


class VolumeLanding(LocalLanding):
    """The landing volume through its /Volumes mount, for when the collector runs on Databricks.

    Files are written straight to their final name: a volume is object storage, where rename is
    a copy, and the manifest written afterwards is what marks a file complete.
    """

    def __init__(self, catalog: str) -> None:
        super().__init__(Path(landing_path(catalog)))

    def _place(self, target: Path, write: Callable[[Path], None]) -> None:
        write(target)

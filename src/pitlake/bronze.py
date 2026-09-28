"""Spark-free helpers for loading landed files into Bronze."""

import hashlib
import shutil
import zipfile
from collections.abc import Iterable, Iterator, Sequence
from datetime import date
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")

_CHUNK = 1024 * 1024


class IntegrityError(Exception):
    """A landed file does not match its manifest. The load must stop, not skip it."""


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(path: str | Path, expected: str) -> None:
    actual = sha256_file(path)
    if actual != expected.lower():
        raise IntegrityError(f"{path}: sha256 {actual} does not match manifest {expected}")


def extract_single_member(archive: str | Path, dest_dir: str | Path) -> Path:
    """Extract the one data file inside a zip archive without loading it into memory.

    Anything other than exactly one regular file is treated as a format change upstream.
    """
    dest_dir = Path(dest_dir)
    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise IntegrityError(f"{archive}: expected exactly one file in archive, got {names}")
        member = members[0]
        # Only the base name is used, so a crafted member path cannot escape dest_dir.
        target = dest_dir / Path(member.filename).name
        dest_dir.mkdir(parents=True, exist_ok=True)
        with zf.open(member) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst, _CHUNK)
    return target


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def replace_where_predicate(source_files: Sequence[str]) -> str:
    """Predicate that scopes an overwrite to exactly the files being (re)loaded."""
    if not source_files:
        raise ValueError("no source files to replace")
    return f"_source_file IN ({', '.join(sql_string(f) for f in source_files)})"


def batched(items: Iterable[T], size: int) -> Iterator[list[T]]:
    if size < 1:
        raise ValueError("batch size must be at least 1")
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def parse_bool(value: str) -> bool:
    """Parse job parameters, which always arrive as strings."""
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise ValueError(f"not a boolean: {value!r}")


def parse_optional_date(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    return date.fromisoformat(value.strip())

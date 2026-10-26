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
    pass


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
    dest_dir = Path(dest_dir)
    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise IntegrityError(f"{archive}: expected exactly one file in archive, got {names}")
        member = members[0]
        target = dest_dir / Path(member.filename).name
        dest_dir.mkdir(parents=True, exist_ok=True)
        with zf.open(member) as src, open(target, "wb") as dst:
            shutil.copyfileobj(src, dst, _CHUNK)
    return target


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_list(values: Iterable[str | date]) -> str:
    return ", ".join(
        f"DATE'{v.isoformat()}'" if isinstance(v, date) else sql_string(v) for v in sorted(values)
    )


# Delta keeps 32 characters of string statistics, a prefix every source path shares (ADR 0013).
def replace_where_predicate(files: Sequence[tuple[str, str, date]]) -> str:
    if not files:
        raise ValueError("no source files to replace")
    if any(symbol is None or period is None for _, symbol, period in files):
        raise ValueError("every source file needs a symbol and a period to replace")
    return (
        f"_symbol IN ({sql_list({s for _, s, _ in files})}) "
        f"AND _period_start IN ({sql_list({p for _, _, p in files})}) "
        f"AND _source_file IN ({sql_list({f for f, _, _ in files})})"
    )


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

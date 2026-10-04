import zipfile
from datetime import date

import pytest

from pitlake.bronze import (
    IntegrityError,
    batched,
    extract_single_member,
    parse_bool,
    parse_optional_date,
    replace_where_predicate,
    sha256_file,
    verify_sha256,
)
from pitlake.datasets import BINANCE_SPOT_TRADES, Dataset, get_dataset
from pitlake.jobs.load_bronze import parse_args
from pitlake.readers import records_schema
from pitlake.tables import bronze_ddl, raw_schema


def _zip(path, members: dict[str, bytes]):
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return path


def test_extract_single_member(tmp_path):
    archive = _zip(tmp_path / "a.zip", {"BTCUSDT-trades-2025-01-01.csv": b"1,2,3\n"})
    extracted = extract_single_member(archive, tmp_path / "out")
    assert extracted.name == "BTCUSDT-trades-2025-01-01.csv"
    assert extracted.read_bytes() == b"1,2,3\n"


def test_extract_rejects_archives_with_several_files(tmp_path):
    archive = _zip(tmp_path / "a.zip", {"a.csv": b"", "b.csv": b""})
    with pytest.raises(IntegrityError):
        extract_single_member(archive, tmp_path / "out")


def test_extract_cannot_escape_destination(tmp_path):
    archive = _zip(tmp_path / "a.zip", {"../../evil.csv": b"x"})
    extracted = extract_single_member(archive, tmp_path / "out")
    assert extracted.parent == tmp_path / "out"


def test_verify_sha256(tmp_path):
    f = tmp_path / "f"
    f.write_bytes(b"hello")
    verify_sha256(f, sha256_file(f).upper())
    with pytest.raises(IntegrityError):
        verify_sha256(f, "0" * 64)


def test_replace_where_predicate_quotes_values():
    assert replace_where_predicate(["a/b.zip", "c'd.zip"]) == (
        "_source_file IN ('a/b.zip', 'c\\'d.zip')"
    )
    with pytest.raises(ValueError):
        replace_where_predicate([])


def test_batched():
    assert list(batched(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert list(batched([], 3)) == []


@pytest.mark.parametrize(("raw", "expected"), [("true", True), (" False ", False), ("", False)])
def test_parse_bool(raw, expected):
    assert parse_bool(raw) is expected


def test_parse_bool_rejects_garbage():
    with pytest.raises(ValueError):
        parse_bool("maybe")


def test_parse_optional_date():
    assert parse_optional_date("") is None
    assert parse_optional_date("2025-02-03") == date(2025, 2, 3)


def test_bronze_keeps_every_raw_column_as_string():
    schema = raw_schema(BINANCE_SPOT_TRADES)
    assert schema.startswith("trade_id STRING, price STRING")
    assert schema.endswith("_corrupt_record STRING")
    ddl = bronze_ddl("pitlake_dev", BINANCE_SPOT_TRADES)
    assert "`pitlake_dev`.`bronze`.`binance_spot_trades`" in ddl
    assert "_source_file STRING" in ddl


def test_dataset_rejects_reserved_column_names():
    with pytest.raises(ValueError):
        Dataset(source="x", name="y", columns=("_source",))


@pytest.mark.parametrize(
    "bad",
    [
        {"archive": "tar"},
        {"file_format": "parquet"},
        {"file_format": "json_records"},
        {"file_format": "csv", "records_path": "rows"},
        {"file_format": "json_records", "records_path": "Bad-Path"},
    ],
)
def test_dataset_rejects_unknown_formats(bad):
    with pytest.raises(ValueError):
        Dataset(source="x", name="y", columns=("a",), **bad)


def test_coinbase_candles_are_read_from_the_landed_file():
    candles = get_dataset("coinbase.spot_candles_1m")
    assert (candles.archive, candles.file_format) == ("none", "json_rows")
    assert candles.bronze_table == "coinbase_spot_candles_1m"
    assert raw_schema(candles).startswith("time STRING, low STRING, high STRING, open STRING")


def test_unknown_dataset():
    with pytest.raises(ValueError):
        get_dataset("nope.nothing")


def test_load_args_accept_empty_job_parameters():
    args = parse_args(["--catalog", "pitlake_dev", "--period-start", "", "--run-id", "123"])
    assert args.period_start is None and args.reload is False and args.run_id == "123"
    assert [d.key for d in args.datasets] == [
        "binance.spot_trades",
        "coinbase.spot_candles_1m",
        "fred.series_vintages",
    ]


def test_reload_without_a_period_is_refused():
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "pitlake_dev", "--reload", "true"])


def test_fred_vintages_read_every_observation_field_as_string():
    fred = get_dataset("fred.series_vintages")
    assert (fred.archive, fred.file_format, fred.records_path) == (
        "none",
        "json_records",
        "observations",
    )
    assert records_schema(fred) == (
        "STRUCT<`observations`: ARRAY<STRUCT<`realtime_start`: STRING, `realtime_end`: STRING, "
        "`date`: STRING, `value`: STRING>>>"
    )

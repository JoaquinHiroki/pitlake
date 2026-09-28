import json
from datetime import date
from pathlib import Path

import pytest

from fakes import FakeFilesApi, FakeHttp
from pitlake.manifest import LandingManifest
from pitlake_collector.config import CollectorConfig, Feed
from pitlake_collector.landing import DatabricksLanding, LocalLanding, UploadError
from pitlake_collector.sources.binance import BASE_URL
from pitlake_collector.sync import run_cycle

TODAY = date(2025, 1, 4)  # so the latest published day is 2025-01-03


def _url(day: str) -> str:
    return f"{BASE_URL}/BTCUSDT/BTCUSDT-trades-{day}.zip"


def _config(tmp_path: Path, **overrides) -> CollectorConfig:
    feed = Feed("binance", "spot_trades", ("BTCUSDT",), date(2025, 1, 1))
    values = dict(catalog="pitlake_dev", work_dir=tmp_path / "work", feeds=(feed,))
    values.update(overrides)
    return CollectorConfig(**values)


def _all_days() -> dict[str, bytes]:
    return {_url(d): d.encode() for d in ("2025-01-01", "2025-01-02", "2025-01-03")}


def test_cycle_lands_files_with_manifests(tmp_path):
    landing = LocalLanding(tmp_path / "landing")
    [result] = run_cycle(_config(tmp_path), landing, FakeHttp(_all_days()), today=TODAY)

    assert (result.missing, result.landed, result.failed) == (3, 3, [])
    feed_dir = tmp_path / "landing/binance/spot_trades/BTCUSDT"
    manifest = LandingManifest.from_json(
        (feed_dir / "BTCUSDT-trades-2025-01-02.zip.manifest.json").read_text()
    )
    assert manifest.period_start == date(2025, 1, 2)
    assert manifest.landing_path == "binance/spot_trades/BTCUSDT/BTCUSDT-trades-2025-01-02.zip"
    assert (feed_dir / "BTCUSDT-trades-2025-01-02.zip").read_bytes() == b"2025-01-02"
    assert list((tmp_path / "work/downloads").iterdir()) == []


def test_second_cycle_fetches_nothing(tmp_path):
    landing = LocalLanding(tmp_path / "landing")
    run_cycle(_config(tmp_path), landing, FakeHttp(_all_days()), today=TODAY)
    http = FakeHttp(_all_days())
    [result] = run_cycle(_config(tmp_path), landing, http, today=TODAY)
    assert (result.missing, result.landed) == (0, 0)
    assert http.requests == []


def test_file_without_manifest_is_fetched_again(tmp_path):
    landing = LocalLanding(tmp_path / "landing")
    run_cycle(_config(tmp_path), landing, FakeHttp(_all_days()), today=TODAY)
    (
        tmp_path / "landing/binance/spot_trades/BTCUSDT/BTCUSDT-trades-2025-01-02.zip.manifest.json"
    ).unlink()
    [result] = run_cycle(_config(tmp_path), landing, FakeHttp(_all_days()), today=TODAY)
    assert result.landed == 1


def test_max_files_limits_a_cycle_oldest_first(tmp_path):
    landing = LocalLanding(tmp_path / "landing")
    [result] = run_cycle(
        _config(tmp_path, max_files_per_cycle=2), landing, FakeHttp(_all_days()), today=TODAY
    )
    assert (result.missing, result.landed) == (3, 2)
    names = landing.list_names("binance/spot_trades/BTCUSDT")
    assert "BTCUSDT-trades-2025-01-03.zip" not in names


def test_unpublished_and_corrupt_files_are_not_landed(tmp_path):
    files = _all_days()
    del files[_url("2025-01-03")]
    http = FakeHttp(files, checksums={_url("2025-01-02"): "0" * 64})
    landing = LocalLanding(tmp_path / "landing")
    [result] = run_cycle(_config(tmp_path), landing, http, today=TODAY)
    assert result.landed == 1
    assert result.not_published == 1
    assert result.failed == ["BTCUSDT-trades-2025-01-02.zip"]
    assert landing.list_names("binance/spot_trades/BTCUSDT") == {
        "BTCUSDT-trades-2025-01-01.zip",
        "BTCUSDT-trades-2025-01-01.zip.manifest.json",
    }


def test_window_overrides_are_clamped_to_the_feed(tmp_path):
    landing = LocalLanding(tmp_path / "landing")
    [result] = run_cycle(
        _config(tmp_path),
        landing,
        FakeHttp(_all_days()),
        start=date(2024, 6, 1),
        end=date(2025, 1, 1),
        today=TODAY,
    )
    assert result.missing == 1


def test_databricks_landing_writes_manifest_after_file(tmp_path):
    files_api = FakeFilesApi()
    landing = DatabricksLanding(files_api, "pitlake_dev")
    [result] = run_cycle(
        _config(tmp_path, max_files_per_cycle=1), landing, FakeHttp(_all_days()), today=TODAY
    )
    assert result.landed == 1
    base = "/Volumes/pitlake_dev/raw/landing/binance/spot_trades/BTCUSDT/"
    assert list(files_api.store) == [
        base + "BTCUSDT-trades-2025-01-01.zip",
        base + "BTCUSDT-trades-2025-01-01.zip.manifest.json",
    ]
    manifest = json.loads(files_api.store[base + "BTCUSDT-trades-2025-01-01.zip.manifest.json"])
    assert manifest["size_bytes"] == len(b"2025-01-01")
    assert base in files_api.directories


def test_truncated_upload_gets_no_manifest(tmp_path):
    files_api = FakeFilesApi(truncate=True)
    landing = DatabricksLanding(files_api, "pitlake_dev")
    [result] = run_cycle(
        _config(tmp_path, max_files_per_cycle=1), landing, FakeHttp(_all_days()), today=TODAY
    )
    assert result.landed == 0 and len(result.failed) == 1
    assert not any(k.endswith(".manifest.json") for k in files_api.store)


def test_databricks_landing_missing_directory_is_empty():
    assert DatabricksLanding(FakeFilesApi(), "pitlake_dev").list_names("binance/x/Y") == set()


def test_upload_error_is_raised_on_size_mismatch(tmp_path):
    local = tmp_path / "f.zip"
    local.write_bytes(b"abc")
    landing = DatabricksLanding(FakeFilesApi(truncate=True), "pitlake_dev")
    with pytest.raises(UploadError):
        landing.put_file("binance/spot_trades/BTCUSDT/f.zip", local)

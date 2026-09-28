from datetime import date
from pathlib import Path

import pytest

from pitlake_collector import cli
from pitlake_collector.config import CollectorConfig, Feed
from pitlake_collector.landing import LocalLanding, VolumeLanding

CONFIG = CollectorConfig(
    catalog="pitlake_dev",
    work_dir=Path("/tmp/w"),
    feeds=(Feed("binance", "spot_trades", ("BTCUSDT",), date(2025, 1, 1)),),
)


def test_volume_mode_writes_to_the_catalogs_landing_volume():
    landing = cli.build_landing(CONFIG, "volume", None)
    assert isinstance(landing, VolumeLanding)
    assert landing.describe() == "/Volumes/pitlake_dev/raw/landing"


def test_landing_dir_wins_over_mode(tmp_path):
    landing = cli.build_landing(CONFIG, "volume", tmp_path)
    assert type(landing) is LocalLanding


def test_volume_landing_writes_final_names_without_temp_files(tmp_path):
    landing = VolumeLanding("pitlake_dev")
    landing._root = tmp_path
    source = tmp_path / "download.zip"
    source.write_bytes(b"data")
    landing.put_file("binance/spot_trades/BTCUSDT/f.zip", source)
    landing.put_text("binance/spot_trades/BTCUSDT/f.zip.manifest.json", "{}")
    assert landing.list_names("binance/spot_trades/BTCUSDT") == {
        "f.zip",
        "f.zip.manifest.json",
    }


def test_entry_raises_so_databricks_marks_the_task_failed(monkeypatch):
    monkeypatch.setattr(cli, "main", lambda: 1)
    with pytest.raises(SystemExit):
        cli.entry()


def test_entry_returns_quietly_on_success(monkeypatch):
    monkeypatch.setattr(cli, "main", lambda: 0)
    cli.entry()

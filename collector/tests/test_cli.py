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


TWO_FEEDS = CollectorConfig(
    catalog="pitlake_dev",
    work_dir=Path("/tmp/w"),
    feeds=(
        Feed("binance", "spot_trades", ("BTCUSDT",), date(2025, 1, 1)),
        Feed("edgar", "filings", ("AAPL",), date(2025, 1, 1)),
    ),
)


def test_feed_limits_the_config_to_one_feed():
    assert cli.select_feed(TWO_FEEDS, "edgar.filings").feeds == (TWO_FEEDS.feeds[1],)
    assert cli.select_feed(TWO_FEEDS, None) is TWO_FEEDS


def test_a_registered_dataset_without_a_feed_is_a_quiet_no_op(tmp_path):
    config = tmp_path / "c.toml"
    config.write_text(
        'catalog = "pitlake_dev"\n[[feeds]]\nsource = "binance"\ndataset = "spot_trades"\n'
        'symbols = ["BTCUSDT"]\nstart_date = 2025-01-01\n'
    )
    argv = ["sync", "--config", str(config), "--landing-dir", str(tmp_path), "--feed", "x.y"]
    assert cli.main(argv) == 0


def test_private_work_dir_is_fresh_and_removed(tmp_path, monkeypatch):
    seen = []

    def fake_run(args, config):
        seen.append(config.work_dir)
        assert config.work_dir.is_dir() and config.work_dir.name.startswith("pitlake-collector-")
        return 0

    monkeypatch.setattr(cli, "_run", fake_run)
    config = tmp_path / "c.toml"
    config.write_text(
        'catalog = "pitlake_dev"\n[[feeds]]\nsource = "binance"\ndataset = "spot_trades"\n'
        'symbols = ["BTCUSDT"]\nstart_date = 2025-01-01\n'
    )
    assert cli.main(["sync", "--config", str(config), "--private-work-dir"]) == 0
    assert not seen[0].exists()

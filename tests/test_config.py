import pytest

from pitlake.config import (
    LAYERS,
    landing_file_path,
    landing_path,
    landing_relpath,
    manifest_name,
    missing_layers,
    schema_fqn,
    table_fqn,
    validate_identifier,
    validate_segment,
    volume_path,
)


def test_landing_path_matches_volume_layout():
    assert landing_path("pitlake_dev") == "/Volumes/pitlake_dev/raw/landing"
    assert landing_path("pitlake_dev", "binance") == "/Volumes/pitlake_dev/raw/landing/binance"


def test_schema_fqn_quotes_identifiers():
    assert schema_fqn("pitlake_prod", "silver") == "`pitlake_prod`.`silver`"


@pytest.mark.parametrize("bad", ["", "Dev", "a-b", "a b", "x; DROP TABLE t", "1abc", "a" * 64])
def test_identifier_rejects_unsafe_names(bad):
    with pytest.raises(ValueError):
        validate_identifier(bad)


def test_unknown_layer_rejected():
    with pytest.raises(ValueError):
        schema_fqn("pitlake_dev", "platinum")


def test_missing_layers_reports_gaps_in_order():
    assert missing_layers(set(LAYERS)) == []
    assert missing_layers({"raw", "gold"}) == ["bronze", "silver", "control"]


def test_landing_relpath_layout():
    assert (
        landing_relpath("binance", "spot_trades", "BTCUSDT", "BTCUSDT-trades-2025-01-01.zip")
        == "binance/spot_trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip"
    )


def test_landing_file_path_is_under_the_volume():
    assert (
        landing_file_path("pitlake_dev", "binance/spot_trades/BTCUSDT/f.zip")
        == "/Volumes/pitlake_dev/raw/landing/binance/spot_trades/BTCUSDT/f.zip"
    )


@pytest.mark.parametrize("bad", ["..", ".hidden", "a/b", "", "x'y", "a b", "a" * 129])
def test_segment_rejects_traversal_and_quotes(bad):
    with pytest.raises(ValueError):
        validate_segment(bad)


def test_landing_file_path_rejects_traversal():
    with pytest.raises(ValueError):
        landing_file_path("pitlake_dev", "binance/../../control/x")


def test_table_fqn_and_volume_path():
    assert table_fqn("pitlake_dev", "control", "bronze_load_log") == (
        "`pitlake_dev`.`control`.`bronze_load_log`"
    )
    assert volume_path("pitlake_dev", "bronze", "staging") == "/Volumes/pitlake_dev/bronze/staging"


def test_manifest_name():
    assert manifest_name("f.zip") == "f.zip.manifest.json"

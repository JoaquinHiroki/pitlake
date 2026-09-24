import pytest

from pitlake.config import LAYERS, landing_path, missing_layers, schema_fqn, validate_identifier


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

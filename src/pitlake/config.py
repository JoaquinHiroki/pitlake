"""Naming conventions shared by jobs, the collector and tests.

Every catalog, schema and path in the platform is derived here so that no other module
hard-codes a name.
"""

import re

ENVIRONMENTS = {"dev": "pitlake_dev", "prod": "pitlake_prod"}
LAYERS = ("raw", "bronze", "silver", "gold", "control")
LANDING_VOLUME = "landing"
STAGING_VOLUME = "staging"
CHECKPOINTS_VOLUME = "checkpoints"

# Sidecar written after a data file lands. Its presence is the commit marker: the platform
# ignores any landed file that has no manifest next to it.
MANIFEST_SUFFIX = ".manifest.json"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
# Path segments may carry symbols and dates (BTCUSDT, 2025-01-01), so they allow upper case,
# dots and dashes, but must start with an alphanumeric so "." and ".." can never appear.
_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def validate_identifier(name: str) -> str:
    """Reject anything that is not a plain lowercase Unity Catalog identifier."""
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def validate_segment(segment: str) -> str:
    """Reject path segments that could escape their directory or break SQL literals."""
    if not _SEGMENT.fullmatch(segment):
        raise ValueError(f"invalid path segment: {segment!r}")
    return segment


def schema_fqn(catalog: str, layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer: {layer!r}")
    return f"`{validate_identifier(catalog)}`.`{layer}`"


def table_fqn(catalog: str, layer: str, table: str) -> str:
    return f"{schema_fqn(catalog, layer)}.`{validate_identifier(table)}`"


def volume_path(catalog: str, layer: str, volume: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer: {layer!r}")
    return f"/Volumes/{validate_identifier(catalog)}/{layer}/{validate_identifier(volume)}"


def landing_path(catalog: str, source: str | None = None) -> str:
    base = volume_path(catalog, "raw", LANDING_VOLUME)
    return base if source is None else f"{base}/{validate_identifier(source)}"


def landing_dir(source: str, dataset: str, symbol: str) -> str:
    """Directory of one feed inside the landing volume: <source>/<dataset>/<symbol>."""
    return "/".join(
        [validate_identifier(source), validate_identifier(dataset), validate_segment(symbol)]
    )


def landing_relpath(source: str, dataset: str, symbol: str, file_name: str) -> str:
    """Path of a landed file relative to the landing volume root."""
    return f"{landing_dir(source, dataset, symbol)}/{validate_segment(file_name)}"


def landing_file_path(catalog: str, relpath: str) -> str:
    segments = relpath.split("/")
    for segment in segments:
        validate_segment(segment)
    return f"{landing_path(catalog)}/{'/'.join(segments)}"


def manifest_name(file_name: str) -> str:
    return validate_segment(file_name) + MANIFEST_SUFFIX


def missing_layers(existing_schemas: set[str]) -> list[str]:
    return [layer for layer in LAYERS if layer not in existing_schemas]

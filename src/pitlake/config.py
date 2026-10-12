import re

ENVIRONMENTS = {"dev": "pitlake_dev", "prod": "pitlake_prod"}
LAYERS = ("raw", "bronze", "silver", "gold", "control")
LANDING_VOLUME = "landing"
STAGING_VOLUME = "staging"
CHECKPOINTS_VOLUME = "checkpoints"

MANIFEST_SUFFIX = ".manifest.json"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def validate_segment(segment: str) -> str:
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
    return "/".join(
        [validate_identifier(source), validate_identifier(dataset), validate_segment(symbol)]
    )


def landing_relpath(source: str, dataset: str, symbol: str, file_name: str) -> str:
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

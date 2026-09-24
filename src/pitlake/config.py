"""Naming conventions shared by jobs, the collector and tests.

Every catalog, schema and path in the platform is derived here so that no other module
hard-codes a name.
"""

import re

ENVIRONMENTS = {"dev": "pitlake_dev", "prod": "pitlake_prod"}
LAYERS = ("raw", "bronze", "silver", "gold", "control")
LANDING_VOLUME = "landing"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def validate_identifier(name: str) -> str:
    """Reject anything that is not a plain lowercase Unity Catalog identifier."""
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def schema_fqn(catalog: str, layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f"unknown layer: {layer!r}")
    return f"`{validate_identifier(catalog)}`.`{layer}`"


def landing_path(catalog: str, source: str | None = None) -> str:
    base = f"/Volumes/{validate_identifier(catalog)}/raw/{LANDING_VOLUME}"
    return base if source is None else f"{base}/{validate_identifier(source)}"


def missing_layers(existing_schemas: set[str]) -> list[str]:
    return [layer for layer in LAYERS if layer not in existing_schemas]

import base64
import os
import re

from pitlake_collector.sources.base import Secrets

_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


def env_name(name: str) -> str:
    return "PITLAKE_SECRET_" + name.upper().replace("-", "_")


def secret_reader(scope: str, client=None) -> Secrets:
    cache: dict[str, str] = {}

    def read(name: str) -> str:
        if not _NAME.fullmatch(name):
            raise ValueError(f"invalid secret name: {name!r}")
        if name not in cache:
            value = os.environ.get(env_name(name))
            if value is None:
                nonlocal client
                if client is None:
                    from databricks.sdk import WorkspaceClient

                    client = WorkspaceClient()
                response = client.secrets.get_secret(scope=scope, key=name)
                value = base64.b64decode(response.value).decode()
            cache[name] = value.strip()
        return cache[name]

    return read

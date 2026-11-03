import argparse
import json
import os
import subprocess
from pathlib import Path

import uvicorn

APP = Path(__file__).resolve().parents[1] / "app"


def bundle_env(target: str) -> dict[str, str]:
    summary = json.loads(
        subprocess.run(
            ["databricks", "bundle", "summary", "-t", target, "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    resources, variables = summary["resources"], summary["variables"]
    spaces = resources.get("genie_spaces", {})
    if not all(spaces.get(k, {}).get("id") for k in ("markets_genie", "operations_genie")):
        raise SystemExit(f"the Genie spaces of {target} are not deployed; run its deploy first")
    return {
        "PITLAKE_CATALOG": variables["catalog"]["value"],
        "DATABRICKS_WAREHOUSE_ID": variables["warehouse_id"]["value"],
        "INGEST_JOB_ID": str(resources["jobs"]["ingest"]["id"]),
        "GENIE_MARKETS_SPACE_ID": spaces["markets_genie"]["id"],
        "GENIE_OPERATIONS_SPACE_ID": spaces["operations_genie"]["id"],
        "CHAT_ENDPOINT": variables["chat_endpoint"]["value"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=["dev", "prod"])
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    os.environ.update(bundle_env(args.target))
    uvicorn.run("server:app", app_dir=str(APP), port=args.port, reload=True, reload_dirs=[str(APP)])


if __name__ == "__main__":
    main()

import argparse
import csv
import hashlib
import json
import re
import time
import urllib.request
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState
from export_backtest_inputs import WAREHOUSE

from pitlake.config import table_fqn, validate_identifier
from pitlake.tables import counterfactual_ddls


def tables() -> list[str]:
    return [re.search(r"`control`\.`(\w+)`", d).group(1) for d in counterfactual_ddls("x")]


def read(w: WorkspaceClient, warehouse_id: str, statement: str) -> tuple[list[str], list[list]]:
    response = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.EXTERNAL_LINKS,
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query failed: {response.status}")
    columns = [c.name for c in response.manifest.schema.columns]
    rows: list[list] = []
    part = response.result
    while part is not None:
        chunk = None
        for link in part.external_links or []:
            with urllib.request.urlopen(link.external_link) as found:
                rows.extend(json.loads(found.read()))
            chunk = link.next_chunk_index
        if chunk is None:
            break
        part = w.statement_execution.get_statement_result_chunk_n(response.statement_id, chunk)
    return columns, rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog")
    parser.add_argument("out_dir")
    args = parser.parse_args()
    catalog = validate_identifier(args.catalog)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    w = WorkspaceClient()
    warehouse_id = next(wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE)
    manifest = {"catalog": catalog, "tables": {}}
    for name in tables():
        columns, rows = read(
            w, warehouse_id, f"SELECT * FROM {table_fqn(catalog, 'control', name)}"
        )
        path = out / f"{name}.csv"
        with path.open("w", newline="") as found:
            writer = csv.writer(found)
            writer.writerow(columns)
            writer.writerows(rows)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest["tables"][name] = {"rows": len(rows), "sha256": digest}
        print(f"{name}: {len(rows):,} rows, sha256 {digest[:12]}")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    print(f"wrote {len(manifest['tables'])} tables of {catalog} to {out}")


if __name__ == "__main__":
    main()

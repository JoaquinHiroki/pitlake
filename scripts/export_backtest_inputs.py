import argparse
import re
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

from pitlake.backtest import DECISION_HOUR, FEATURES, UNIVERSE
from pitlake.config import table_fqn, validate_identifier
from pitlake.gold import GLOBAL_ENTITY
from pitlake.tables import BACKTEST_RUNS
from pitlake.tables import FEATURES as FEATURES_TABLE

WAREHOUSE = "Serverless Starter Warehouse"


def query(w: WorkspaceClient, warehouse_id: str, statement: str) -> list[list]:
    response = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query failed: {response.status}")
    rows = list(response.result.data_array or [])
    chunk = response.result.next_chunk_index
    while chunk is not None:
        part = w.statement_execution.get_statement_result_chunk_n(response.statement_id, chunk)
        rows.extend(part.data_array or [])
        chunk = part.next_chunk_index
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog")
    parser.add_argument("backtest_id")
    parser.add_argument("out_dir")
    args = parser.parse_args()
    catalog = validate_identifier(args.catalog)
    backtest = args.backtest_id
    if not re.fullmatch(r"[0-9a-f]{32}", backtest):
        raise SystemExit(f"not a backtest id: {backtest!r}")

    w = WorkspaceClient()
    warehouse_id = next(wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE)
    record = query(
        w,
        warehouse_id,
        "SELECT table_versions['gold.features'], CAST(period_start AS STRING), "
        "CAST(period_end AS STRING), input_hash "
        f"FROM {table_fqn(catalog, 'control', BACKTEST_RUNS)} WHERE backtest_id = '{backtest}'",
    )
    if not record:
        raise SystemExit(f"no backtest {backtest} in {catalog}")
    version, start, end, input_hash = record[0]

    entities = ", ".join(f"'{e}'" for e in [*(a.entity for a in UNIVERSE), GLOBAL_ENTITY])
    features = ", ".join(f"'{f}'" for f in FEATURES)
    rows = query(
        w,
        warehouse_id,
        "SELECT entity, feature, unix_micros(event_time), unix_micros(knowable_at), "
        "CAST(value AS STRING), source_table, source_ref "
        f"FROM {table_fqn(catalog, 'gold', FEATURES_TABLE)} VERSION AS OF {int(version)} "
        f"WHERE feature IN ({features}) AND entity IN ({entities})",
    )

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    meta = {
        "backtest_id": backtest,
        "version": version,
        "period_start": start,
        "period_end": end,
        "input_hash": input_hash,
        "universe": ",".join(a.entity for a in UNIVERSE),
        "global_entity": GLOBAL_ENTITY,
        "decision_hour": str(DECISION_HOUR),
    }
    (out / "meta.tsv").write_text("".join(f"{k}\t{v}\n" for k, v in meta.items()))
    (out / "features.tsv").write_text(
        "".join("\t".join("" if v is None else str(v) for v in row) + "\n" for row in rows)
    )
    print(f"wrote {len(rows)} Gold rows of version {version} for backtest {backtest} to {out}")


if __name__ == "__main__":
    main()

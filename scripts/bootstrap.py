import argparse
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sql import StatementState

from pitlake.bronze import sql_string
from pitlake.config import validate_identifier

WAREHOUSE = "Serverless Starter Warehouse"
CATALOGS = ("pitlake_dev", "pitlake_prod")


def run(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    response = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        raise SystemExit(f"failed: {statement}\n{error.message if error else response.status}")


def exists(w: WorkspaceClient, catalog: str) -> bool:
    try:
        w.catalogs.get(catalog)
    except NotFound:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("catalogs", nargs="*", default=CATALOGS)
    args = parser.parse_args()
    catalogs = [validate_identifier(c) for c in args.catalogs]

    w = WorkspaceClient()
    warehouse_id = None
    for catalog in catalogs:
        if exists(w, catalog):
            print(f"exists:  {catalog}")
            continue
        warehouse_id = warehouse_id or next(
            wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE
        )
        comment = sql_string(f"PITLake {catalog.removeprefix('pitlake_')} environment")
        run(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}` COMMENT {comment}")
        print(f"created: {catalog}")


if __name__ == "__main__":
    main()

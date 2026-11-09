import argparse
import json
import re
from collections import Counter
from pathlib import Path

from databricks.sdk import WorkspaceClient
from export_backtest_inputs import WAREHOUSE, query

from pitlake.backtest import DECISION_HOUR, FEATURES, UNIVERSE
from pitlake.config import table_fqn, validate_identifier
from pitlake.gold import GLOBAL_ENTITY
from pitlake.tables import ABLATION_CHANGES, ABLATIONS, BARS_1D
from pitlake.tables import FEATURES as FEATURES_TABLE

ROW = (
    "entity, feature, unix_micros(event_time), unix_micros(knowable_at), CAST(value AS STRING), "
    "source_table, source_ref"
)


def apply(rows: list[list], changes: list[list]) -> list[tuple]:
    world = Counter(tuple(r) for r in rows)
    for change, *row in changes:
        row = tuple(row)
        if change == "removed":
            if not world[row]:
                raise SystemExit(f"the ablation removed a row the base does not have: {row}")
            world[row] -= 1
        else:
            world[row] += 1
    return sorted(world.elements(), key=repr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("catalog")
    parser.add_argument("ablation_id")
    parser.add_argument("out_dir")
    args = parser.parse_args()
    catalog = validate_identifier(args.catalog)
    ablation = args.ablation_id
    if not re.fullmatch(r"[0-9a-f]{32}", ablation):
        raise SystemExit(f"not an ablation id: {ablation!r}")

    w = WorkspaceClient()
    warehouse_id = next(wh.id for wh in w.warehouses.list() if wh.name == WAREHOUSE)
    record = query(
        w,
        warehouse_id,
        "SELECT backtest_ids[0], table_versions['gold.features'], table_versions['gold.bars_1d'], "
        "CAST(period_start AS STRING), CAST(period_end AS STRING), ablated_input_hash, "
        "to_json(withheld) "
        f"FROM {table_fqn(catalog, 'control', ABLATIONS)} WHERE ablation_id = '{ablation}'",
    )
    if not record:
        raise SystemExit(f"no ablation {ablation} in {catalog}")
    backtest, version, bars_version, start, end, input_hash, withheld = record[0]
    withheld = json.loads(withheld)

    symbols = [a.entity for a in UNIVERSE]
    entities = ", ".join(f"'{e}'" for e in [*symbols, GLOBAL_ENTITY])
    features = ", ".join(f"'{f}'" for f in FEATURES)
    base = query(
        w,
        warehouse_id,
        f"SELECT {ROW} FROM {table_fqn(catalog, 'gold', FEATURES_TABLE)} "
        f"VERSION AS OF {int(version)} WHERE feature IN ({features}) AND entity IN ({entities})",
    )
    changes = query(
        w,
        warehouse_id,
        f"SELECT change, {ROW} FROM {table_fqn(catalog, 'control', ABLATION_CHANGES)} "
        f"WHERE ablation_id = '{ablation}'",
    )
    gone = {tuple(i.removeprefix("bar:").split("/")) for i in withheld if i.startswith("bar:")}
    bars = [
        row
        for row in query(
            w,
            warehouse_id,
            "SELECT symbol, CAST(session_date AS STRING), CAST(close AS STRING), "
            "CAST(volume AS STRING), unix_micros(knowable_at) "
            f"FROM {table_fqn(catalog, 'gold', BARS_1D)} VERSION AS OF {int(bars_version)} "
            f"WHERE symbol IN ({', '.join(repr(s) for s in symbols)})",
        )
        if (row[0], row[1]) not in gone
    ]
    world = apply(base, changes)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    meta = {
        "backtest_id": backtest,
        "ablation_id": ablation,
        "version": version,
        "period_start": start,
        "period_end": end,
        "input_hash": input_hash,
        "universe": ",".join(symbols),
        "global_entity": GLOBAL_ENTITY,
        "decision_hour": str(DECISION_HOUR),
    }
    (out / "meta.tsv").write_text("".join(f"{k}\t{v}\n" for k, v in meta.items()))

    def lines(rows) -> str:
        return "".join("\t".join("" if v is None else str(v) for v in r) + "\n" for r in rows)

    (out / "features.tsv").write_text(lines(world))
    (out / "bars.tsv").write_text(lines(bars))
    (out / "withheld.tsv").write_text("".join(f"{i}\n" for i in withheld) or "\n")
    print(
        f"wrote ablation {ablation}: {len(base)} Gold rows of version {version}, "
        f"{len(changes)} changes, {len(world)} rows in the ablated world, {len(bars)} bars, "
        f"withheld {' '.join(withheld) or 'nothing'} to {out}"
    )


if __name__ == "__main__":
    main()

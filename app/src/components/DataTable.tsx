import { Table } from "../api";

const NUMERIC = new Set(["BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DECIMAL"]);

function format(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

type Props = {
  table: Table;
  onSelect?: (row: unknown[]) => void;
  selected?: unknown;
};

export function DataTable({ table, onSelect, selected }: Props) {
  if (table.rows.length === 0) return <p className="muted">No rows.</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {table.columns.map((c) => (
              <th key={c.name} className={NUMERIC.has(c.type ?? "") ? "num" : undefined}>
                {c.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((row, i) => (
            <tr
              key={i}
              onClick={onSelect && (() => onSelect(row))}
              className={[onSelect && "clickable", selected === row[0] && "selected"]
                .filter(Boolean)
                .join(" ")}
            >
              {row.map((value, j) => (
                <td key={j} className={NUMERIC.has(table.columns[j].type ?? "") ? "num" : undefined}>
                  {format(value)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {table.truncated && (
        <p className="muted">
          Showing {table.rows.length} of {table.row_count} rows.
        </p>
      )}
    </div>
  );
}

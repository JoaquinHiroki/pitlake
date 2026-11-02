import { ReactNode, useState } from "react";
import { Table } from "../api";
import { DataTable } from "./DataTable";

type Props = {
  title: string;
  caption?: ReactNode;
  view: { table: Table | null; error: string | null; loading: boolean; reload: () => void };
  controls?: ReactNode;
  empty?: string;
  onSelect?: (row: unknown[]) => void;
  selected?: unknown;
};

export function ViewSection({ title, caption, view, controls, empty, onSelect, selected }: Props) {
  return (
    <section>
      <header className="section-head">
        <h2>{title}</h2>
        <button onClick={view.reload} disabled={view.loading}>
          {view.loading ? "Loading…" : "Refresh"}
        </button>
      </header>
      {caption && <p className="muted">{caption}</p>}
      {controls}
      {view.error && <p className="error">{view.error}</p>}
      {view.table &&
        (view.table.rows.length === 0 && empty ? (
          <p className="muted">{empty}</p>
        ) : (
          <DataTable table={view.table} onSelect={onSelect} selected={selected} />
        ))}
    </section>
  );
}

export function DaysInput({ value, onChange }: { value: number; onChange: (days: number) => void }) {
  const [draft, setDraft] = useState(value);
  const commit = () => draft !== value && onChange(draft);
  return (
    <label className="inline">
      Days up to the newest data
      <input
        type="range"
        min={7}
        max={365}
        value={draft}
        onChange={(e) => setDraft(Number(e.target.value))}
        onPointerUp={commit}
        onKeyUp={commit}
      />
      <span>{draft}</span>
    </label>
  );
}

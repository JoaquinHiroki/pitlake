import { useCallback, useEffect, useState } from "react";
import { api, Table } from "./api";

export function useView(name: string, params: Record<string, string | number> = {}, skip = false) {
  const [table, setTable] = useState<Table | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const key = JSON.stringify(params);

  const load = useCallback(
    async (refresh = false) => {
      setLoading(true);
      setError(null);
      try {
        setTable(await api.view(name, JSON.parse(key), refresh));
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setLoading(false);
      }
    },
    [name, key],
  );

  useEffect(() => {
    if (!skip) load();
  }, [load, skip]);

  return { table, error, loading, reload: () => load(true) };
}

export function column(table: Table | null, name: string): unknown[] {
  if (!table) return [];
  const index = table.columns.findIndex((c) => c.name === name);
  return index < 0 ? [] : table.rows.map((row) => row[index]);
}

export function records(table: Table | null): Record<string, unknown>[] {
  if (!table) return [];
  return table.rows.map((row) => Object.fromEntries(table.columns.map((c, i) => [c.name, row[i]])));
}

export function pick(
  table: Table,
  names: string[],
  keep: (row: Record<string, unknown>) => boolean = () => true,
): Table {
  const indexes = names
    .map((n) => table.columns.findIndex((c) => c.name === n))
    .filter((i) => i >= 0);
  const all = records(table);
  const rows = table.rows.filter((_, i) => keep(all[i]));
  return {
    ...table,
    columns: indexes.map((i) => table.columns[i]),
    rows: rows.map((row) => indexes.map((i) => row[i])),
  };
}

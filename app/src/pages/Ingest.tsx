import { FormEvent, useCallback, useEffect, useState } from "react";
import { api, IngestRun } from "../api";
import { records, useView } from "../useView";

const isoDay = (offset: number) => {
  const day = new Date();
  day.setDate(day.getDate() + offset);
  return day.toISOString().slice(0, 10);
};

export function Ingest() {
  const registry = useView("registry");
  const datasets = records(registry.table);
  const [chosen, setChosen] = useState<string[]>([]);
  const [ranged, setRanged] = useState(false);
  const [start, setStart] = useState(isoDay(-7));
  const [end, setEnd] = useState(isoDay(-1));
  const [reload, setReload] = useState(false);
  const [maxFiles, setMaxFiles] = useState(0);
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<{ ok: boolean; text: string; url?: string } | null>(null);
  const [runs, setRuns] = useState<IngestRun[]>([]);
  const [runsError, setRunsError] = useState<string | null>(null);

  const loadRuns = useCallback(async () => {
    setRunsError(null);
    try {
      setRuns(await api.ingestRuns());
    } catch (e) {
      setRunsError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    loadRuns();
  }, [loadRuns]);

  const toggle = (dataset: string) =>
    setChosen((c) => (c.includes(dataset) ? c.filter((d) => d !== dataset) : [...c, dataset]));

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setOutcome(null);
    try {
      const run = await api.ingest({
        datasets: chosen,
        start: ranged ? start : null,
        end: ranged ? end : null,
        reload,
        max_files: maxFiles,
      });
      setOutcome({ ok: true, text: `Started run ${run.run_id}.`, url: run.url });
      loadRuns();
    } catch (e) {
      setOutcome({ ok: false, text: (e as Error).message });
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <section>
        <h2>Run the ingestion job</h2>
        {registry.error && <p className="error">{registry.error}</p>}
        <form className="ingest" onSubmit={submit}>
          <fieldset>
            <legend>Datasets</legend>
            {datasets.map((d) => (
              <label key={String(d.dataset)} title={String(d.description ?? "")}>
                <input
                  type="checkbox"
                  checked={chosen.includes(String(d.dataset))}
                  onChange={() => toggle(String(d.dataset))}
                />
                {String(d.dataset)}
              </label>
            ))}
          </fieldset>
          <label>
            <input type="checkbox" checked={ranged} onChange={(e) => setRanged(e.target.checked)} />
            Only a date range
          </label>
          {ranged && (
            <div className="controls">
              <label>
                First day
                <input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
              </label>
              <label>
                Last day
                <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
              </label>
            </div>
          )}
          <label>
            <input type="checkbox" checked={reload} onChange={(e) => setReload(e.target.checked)} />
            Reload files already in Bronze for that range
          </label>
          <label>
            Files per symbol (0 = no limit)
            <input
              type="number"
              min={0}
              max={10000}
              value={maxFiles}
              onChange={(e) => setMaxFiles(Number(e.target.value))}
            />
          </label>
          <button type="submit" disabled={busy}>
            {busy ? "Starting…" : "Start run"}
          </button>
        </form>
        {outcome && (
          <p className={outcome.ok ? "success" : "error"}>
            {outcome.text}{" "}
            {outcome.url && (
              <a href={outcome.url} target="_blank" rel="noreferrer">
                Open in Databricks
              </a>
            )}
          </p>
        )}
      </section>

      <section>
        <header className="section-head">
          <h2>Recent runs</h2>
          <button onClick={loadRuns}>Refresh</button>
        </header>
        {runsError && <p className="error">{runsError}</p>}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>run_id</th>
                <th>started</th>
                <th>state</th>
                <th>result</th>
                <th>parameters</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.run_id}>
                  <td>{r.run_id}</td>
                  <td>{r.started}</td>
                  <td>{r.state}</td>
                  <td>{r.result}</td>
                  <td>
                    {Object.entries(r.parameters)
                      .filter(([, v]) => v)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(" ")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

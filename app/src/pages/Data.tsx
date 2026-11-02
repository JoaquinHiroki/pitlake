import { useState } from "react";
import { DaysInput, ViewSection } from "../components/ViewSection";
import { column, pick, records, useView } from "../useView";

const names = (table: { columns: { name: string }[] }) => table.columns.map((c) => c.name);

export function Coverage() {
  const freshness = useView("freshness");
  const coverage = useView("coverage");
  const tables = [...new Set(column(coverage.table, "silver_table") as string[])].sort();
  const [chosen, setChosen] = useState<string | null>(null);
  const [days, setDays] = useState(90);
  const silverTable = chosen ?? tables[0] ?? "";
  const daily = useView("daily_rows", { silver_table: silverTable, days }, !silverTable);

  const fresh = records(freshness.table);
  const stale = fresh.filter((r) => r.stale).length;
  const checked = fresh[0]?.checked_at as string | undefined;

  return (
    <>
      <ViewSection
        title="Freshness"
        caption={checked && `Checked ${checked} UTC: ${stale} of ${fresh.length} tables stale.`}
        view={freshness}
        empty="The operations job has not checked freshness yet."
      />
      <ViewSection title="Data present per source and period" view={coverage} />
      <ViewSection
        title="Rows per period"
        view={daily}
        empty="No files in that window."
        controls={
          <div className="controls">
            <select value={silverTable} onChange={(e) => setChosen(e.target.value)}>
              {tables.map((t) => (
                <option key={t}>{t}</option>
              ))}
            </select>
            <DaysInput value={days} onChange={setDays} />
          </div>
        }
      />
    </>
  );
}

export function Rejections() {
  const [days, setDays] = useState(30);
  const rates = useView("rejections", { days });
  return (
    <ViewSection
      title="Rejection rates by rule"
      caption="Rows each drop rule rejected, over each table's files up to its newest one."
      view={rates}
      empty="No files in that window."
      controls={<DaysInput value={days} onChange={setDays} />}
    />
  );
}

export function Venues() {
  const [days, setDays] = useState(60);
  const compared = useView("cross_venue", { days });
  const gate = useView("gate");
  const checks = records(gate.table);
  const first = checks[0];
  const failed = checks.filter((c) => !c.passed && !c.waived).length;
  const waived = checks.filter((c) => c.waived).length;
  const flagged = gate.table && {
    ...gate,
    table: pick(
      gate.table,
      names(gate.table).filter((n) => n !== "run_id" && n !== "checked_at"),
      (c) => !c.passed || !!c.waived,
    ),
  };

  return (
    <>
      <ViewSection
        title="Disagreements between the crypto venues"
        view={compared}
        empty="No compared days in that window."
        controls={<DaysInput value={days} onChange={setDays} />}
      />
      <ViewSection
        title="Latest quality gate"
        caption={
          first
            ? `Run ${first.run_id} at ${first.checked_at} UTC: ${checks.length} checks, ` +
              `${waived} waived, ${failed} failed.`
            : undefined
        }
        view={flagged ?? gate}
        empty={first ? "Every check passed without a waiver." : "The gate has not run yet."}
      />
    </>
  );
}

const BACKTEST_LIST = [
  "backtest_id",
  "created_at",
  "strategy",
  "period_start",
  "period_end",
  "sharpe",
  "total_return",
  "annual_volatility",
  "max_drawdown",
  "replay_of",
  "reproduced",
];

export function Backtests() {
  const runs = useView("backtests");
  const [chosen, setChosen] = useState<unknown>(null);
  const detail = records(runs.table).find((r) => r.backtest_id === chosen);
  const listing = runs.table && { ...runs, table: pick(runs.table, BACKTEST_LIST) };

  return (
    <>
      <ViewSection
        title="Backtest runs"
        caption="Choose a run to see its parameters, table versions, code commit and hashes."
        view={listing ?? runs}
        empty="No backtests yet."
        onSelect={(row) => setChosen(row[0])}
        selected={chosen}
      />
      {detail && (
        <section>
          <h2>{String(detail.backtest_id)}</h2>
          <dl className="detail">
            {Object.entries(detail)
              .filter(([key]) => !BACKTEST_LIST.includes(key))
              .map(([key, value]) => (
                <div key={key}>
                  <dt>{key}</dt>
                  <dd>
                    {typeof value === "object" && value !== null ? (
                      <pre>{JSON.stringify(value, null, 2)}</pre>
                    ) : (
                      String(value ?? "—")
                    )}
                  </dd>
                </div>
              ))}
          </dl>
        </section>
      )}
    </>
  );
}

export function Performance({ catalog }: { catalog: string }) {
  const [days, setDays] = useState(30);
  const stats = useView("performance", { days });
  const workloads = [...new Set(column(stats.table, "workload") as string[])].sort();
  const fallback = `pitlake-transform-${catalog.replace(/^pitlake_/, "")}`;
  const [chosen, setChosen] = useState<string | null>(null);
  const workload = chosen ?? (workloads.includes(fallback) ? fallback : workloads[0]);
  const job = stats.table && {
    ...stats,
    table: pick(
      stats.table,
      names(stats.table).filter((n) => n !== "workload"),
      (r) => r.workload === workload,
    ),
  };

  return (
    <ViewSection
      title="Query cost per job and day"
      caption={
        "From the workspace's query history, recorded daily by the operations job. Files skipped " +
        "are those Delta ruled out from statistics without reading them (ADR 0013)."
      }
      view={job ?? stats}
      empty="The operations job has not recorded query statistics yet."
      controls={
        <div className="controls">
          <select value={workload ?? ""} onChange={(e) => setChosen(e.target.value)}>
            {workloads.map((w) => (
              <option key={w}>{w}</option>
            ))}
          </select>
          <DaysInput value={days} onChange={setDays} />
        </div>
      }
    />
  );
}

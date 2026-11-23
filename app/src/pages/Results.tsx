import { useState } from "react";
import { ViewSection } from "../components/ViewSection";
import { pick, records, useView } from "../useView";

const DECISION_LIST = [
  "point_key",
  "decider",
  "decision_at",
  "settled",
  "items",
  "worlds",
  "screened",
  "causing",
  "null_changes_nothing",
  "equal_to_rebuild",
  "differ_from_rebuild",
  "reproducible",
];

const SET_LIST = [
  "point_key",
  "decider",
  "decision_at",
  "candidates",
  "kept_count",
  "kept_by_kind",
  "tests",
  "sampled",
  "confirmed",
  "rebuilt",
  "coherent",
  "ablation_id",
];

const SOURCE_LIST = [
  "decider",
  "source",
  "level",
  "verdict",
  "decisions",
  "changed",
  "parts",
  "parts_changed",
  "largest_effect",
  "knowable",
  "attributed",
  "items_caused",
  "parts_caused",
];

export function Results() {
  const decisions = useView("attribution");
  const [point, setPoint] = useState<string | null>(null);
  const causes = useView("causes", { point_key: point ?? "" }, !point);
  const chosen = records(decisions.table).find((r) => r.point_key === point);

  const sets = useView("sufficiency");
  const [setKey, setSetKey] = useState<string | null>(null);
  const kept = records(sets.table).find((r) => r.point_key === setKey);

  const sources = useView("sources");
  const backtests = [...new Set(records(sources.table).map((r) => String(r.backtest_id)))];

  return (
    <>
      <ViewSection
        title="Which information caused each decision"
        caption={
          "Every item knowable at the decision was withheld one at a time and the world rebuilt " +
          "without it (ADR 0025). Causing counts the items whose removal changed a settled part; " +
          "the null world withholds nothing and must change nothing. Choose a decision to see " +
          "its causing items."
        }
        view={decisions.table ? { ...decisions, table: pick(decisions.table, DECISION_LIST) } : decisions}
        empty="No decision has been attributed yet."
        onSelect={(row) => setPoint(String(row[0]))}
        selected={point}
      />
      {chosen && (
        <ViewSection
          title={`Items that change the ${chosen.decider} decision at ${chosen.decision_at}`}
          caption={`Its decision in the real world: ${chosen.base_decision}`}
          view={causes}
          empty="Withholding any one item leaves this decision as it was."
        />
      )}
      <ViewSection
        title="The least information that reproduces each decision"
        caption={
          "A minimal sufficient set: the decision is the same with only these items, and dropping " +
          "any one of them changes it (ADR 0026). Confirmed means it was decided again from the " +
          "set alone; coherent means that world passed the independent check. Choose one to see " +
          "its items."
        }
        view={sets.table ? { ...sets, table: pick(sets.table, SET_LIST) } : sets}
        empty="No sufficient set has been found yet."
        onSelect={(row) => setSetKey(String(row[0]))}
        selected={setKey}
      />
      {kept && (
        <section>
          <h2>
            {String(kept.kept_count)} items reproduce the {String(kept.decider)} decision at{" "}
            {String(kept.decision_at)}
          </h2>
          <p className="muted">Its decision in the real world: {String(kept.base_decision)}</p>
          <pre>{((kept.kept as string[] | null) ?? []).join("\n") || "No item: it holds with none."}</pre>
        </section>
      )}
      {backtests.map((backtest) => (
        <ViewSection
          key={backtest}
          title={`Which sources change decisions in backtest ${backtest.slice(0, 8)}`}
          caption={
            "Each feed, and each series of a feed, withheld whole (ADR 0030). Changes nothing " +
            "means no part of any decision moved; attributed and items caused are what " +
            "withholding its items one at a time found."
          }
          view={
            sources.table
              ? {
                  ...sources,
                  table: pick(sources.table, SOURCE_LIST, (r) => String(r.backtest_id) === backtest),
                }
              : sources
          }
        />
      ))}
      {backtests.length === 0 && (
        <ViewSection
          title="Which sources change decisions"
          view={sources}
          empty="No source has been valued yet."
        />
      )}
    </>
  );
}

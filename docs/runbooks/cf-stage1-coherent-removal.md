# Counterfactual Stage 1 runbook: coherent removal

The `counterfactual` job's `ablate` task withholds items from Gold, recomputes what depended on
them, and fails unless every ablated world equals a full rebuild without those items, keeps no row
of them, and accounts for every value the backtest reads in it. `make ablationcheck` then checks a
world with the independent OCaml program ([ADR 0023](../adr/0023-coherent-removal.md)). Stage 1 is
complete when that holds in prod and the cost per ablation is known.

The `enumerate` task of Stage 0 runs first in every run. Follow the parts in order. If you see
something other than what a step says, stop and keep the full output. `make ablationcheck` needs
OCaml (`eval $(opam env)` first) and a login to the workspace.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes, and `Deployment complete!`.

### A2. Withhold a sample

```bash
databricks bundle run counterfactual -t dev --params sample=2
```

✅ `TERMINATED SUCCESS`. `enumerate` prints 24 `COMPLETE` lines as in Stage 0. `ablate` prints the
period and set, then one line per world starting with `COHERENT`: first `withheld nothing (null
world)` with inputs `unchanged`, then two bars and the two filings that were read, each with the rows
removed and recomputed, `matches the full rebuild`, `0 owned left`, `0 unaccounted` and its
seconds, and ends `OK: 5 ablated worlds in pitlake_dev, every one coherent`. Dev has no 20-day
volatility (two weeks of bars), so a bar recomputes only the closes after it.

### A3. Check a world independently

In the SQL editor:

```sql
SELECT ablation_id, withheld, rows_removed, rows_recomputed, inputs_changed, coherent,
       removal_seconds, inputs_seconds, rebuild_seconds
FROM pitlake_dev.control.ablations
WHERE run_id = (SELECT max_by(run_id, created_at) FROM pitlake_dev.control.ablations)
ORDER BY created_at
```

✅ Five rows, all `coherent = true`. Take the `ablation_id` of a bar, then:

```bash
eval $(opam env)
make ablationcheck TARGET=dev ABLATION=<ablation ID>
```

✅ `IDENTICAL: an independent implementation of as-of sees exactly what the backtest saw`, then
`withheld bar:...`, the number of price rows recomputed, and `COHERENT: no row comes from a
withheld item and every price feature equals its bars`.

---

## Part B: prod and the Stage 1 exit test

### B1. Deploy and withhold five read items of each kind

```bash
make deploy-prod
databricks bundle run counterfactual -t prod --params backtest=238695896043288,sample=5
```

About 15 to 25 minutes. ✅ `TERMINATED SUCCESS`: 24 `COMPLETE` lines, then 16 `COHERENT` lines
(the null world with inputs `unchanged`, then five bars, five filings and five CPI vintages) and
`OK: 16 ablated worlds in pitlake_prod, every one coherent`. Send Claude the run ID.

### B2. Withhold several items as one world

```bash
databricks bundle run counterfactual -t prod --params "backtest=238695896043288,withhold=filing:0001045810-26-000075 bar:AAPL/2026-06-15"
```

✅ `TERMINATED SUCCESS`, with a `COHERENT` line that withheld both items, inputs `changed`.

### B3. Drill: hide a bar without recomputing

```bash
databricks bundle run counterfactual -t prod --params backtest=238695896043288,withhold=bar:AAPL/2026-06-15,mode=hide
```

✅ The run **fails**: `enumerate` succeeds, and `ablate` prints `INCOHERENT: withheld
bar:AAPL/2026-06-15: removed 2 rows, recomputed 0; ...; +N/-M against the full rebuild`, with
sample lines of `volatility_20d` rows of the following sessions, and the error says
`1 ablated worlds are not coherent`.

### B4. Check worlds independently

Run the query of A3 with `pitlake_prod`, without the `WHERE run_id` line, and add `ablation_mode`
to the columns. Then, for the world of B2, one bar, one filing and one release of B1, and the drill
of B3:

```bash
make ablationcheck TARGET=prod ABLATION=<ablation ID>
```

✅ Every coherent world gives `IDENTICAL` and `COHERENT`. The drill gives `INCOHERENT`, with lines
such as `volatility_20d of AAPL for 2026-06-16 is ..., the bars give ...`, and the command fails.
Send Claude the output.

**Stage 1 is complete** when B1 and B2 hold, the drill fails in the job and in `ablationcheck`, and
every coherent world passes `ablationcheck`. Claude then records the cost per ablation from
`control.ablations` in ADR 0023.

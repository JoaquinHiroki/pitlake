# Counterfactual Stage 5 runbook: horizon

The `horizon` job decides, for each decision, in worlds that keep only the items that became
public within a horizon of days before its instant, over a ladder of horizons from a year to
none: the degradation curve. It then halves the gap to the day where the decision stops being the
same, records the worlds either side of that truncation point as ablations for the independent
check, and prints the curve per decider ([ADR 0027](../adr/0027-horizon.md)). A run stops starting
worlds after `minutes` and the same command continues it. Stage 5 is complete when every decision
of the prod set is `MEASURED`, each with its truncation point, and every boundary world passes
`make ablationcheck`.

Stage 2 must be complete first: the prod points of
[cf-stage2-decision-harness.md](cf-stage2-decision-harness.md) B1 to B4 reproducible. Stages 3 and
4 are not needed; a decision they already recorded with the same `samples` and `min_agreement` is
reused. Follow the parts in order. If you see something other than what a step says, stop and
keep the full output. Every time is UTC.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes and `Deployment complete!`.

### A2. The strategy, stopped early

```bash
databricks bundle run horizon -t dev --params deciders=strategy,minutes=0
```

✅ `TERMINATED SUCCESS` with a line starting `PARTIAL: strategy at 2025-01-15 06:00 UTC`, then
either `stopped before the curve was complete` with `0 worlds decided`, or, if no earlier stage
decided it in dev, `no decision in the real world yet`; and last
`PARTIAL: 1 of 1 decisions still have worlds to decide`.

### A3. The same command continues it

```bash
databricks bundle run horizon -t dev --params deciders=strategy
```

✅ `MEASURED: strategy at 2025-01-15 06:00 UTC` with
`reproducible at every horizon down to 0 days`, one line per horizon, all `same`, the last
`0 days` with `holds: ablation <ID> coherent`, then
`each part holds down to (days): AAPL 0, BTCUSDT 0, MSFT 0, NVDA 0, SPY 0`, a `CURVE: strategy`
block, and last `OK: 1 decisions in pitlake_dev have a degradation curve`. In dev every weight at
2025-01-15 is 0 (Stage 3's runbook says why), and a world with nothing in it gives weights of 0,
so no horizon changes it. Keep the ablation ID.

### A4. The independent check of the boundary world

```bash
eval $(opam env)
make ablationcheck TARGET=dev ABLATION=<ablation ID from A3>
```

✅ `IDENTICAL` and `COHERENT`.

### A5. The agent

```bash
databricks bundle run horizon -t dev --params deciders=agent,samples=3
```

Up to an hour. ✅ `MEASURED: agent at 2025-01-15 06:00 UTC` with the ratings that settled,
`worlds decided` with a number `sampled` above 0, a `reproducible` line, and each boundary
`coherent`. If the last line is `PARTIAL`, run the same command again. ❌ `FAILED` with
`not reproducible`: no rating settled in the real world; that is a finding, not a bug. Send Claude
the output.

---

## Part B: prod and the Stage 5 exit test

The decision set is Stage 3's: the strategy and the agent at three instants of backtest
`a4d4c4e0955046b8a6fb6a171f8b1071`, 2026-06-01, 2026-08-27 and 2026-10-08.

### B1. The strategy

```bash
make deploy-prod
databricks bundle run horizon -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=strategy,at=2026-06-01 2026-08-27 2026-10-08"
```

Under half an hour. ✅ Three `MEASURED` lines, each with `reproducible down to <N> days, not at
<N - 1> days`, where N is the age of the 21st-last bar of the symbols it holds: 29 to 32 when it
holds an equity, 21 when it holds only BTCUSDT. Each part's horizon is that age for a symbol with a
weight above zero and 0 for the others. Both boundaries `coherent`, then the `CURVE: strategy`
block. If the last line is `PARTIAL`, run the same command again until it is `OK`. Send Claude
every run ID and the output.

### B2. The agent

```bash
databricks bundle run horizon -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=agent,at=2026-06-01 2026-08-27 2026-10-08"
```

Up to two hours per run. Run the same command again while the last line is `PARTIAL`; each run
reuses the worlds already decided. ✅ Three `MEASURED` lines, the `CURVE: agent` block and
`OK: 3 decisions`. Send Claude every run ID and the output.

### B3. The independent check of every boundary world

```sql
SELECT decider, decision_at, horizon_days, boundary, ablation_id, coherent
FROM pitlake_prod.control.horizon_curves
WHERE boundary IS NOT NULL
ORDER BY decider, decision_at, horizon_days DESC
```

Up to twelve rows, two per decision (one when it holds at every horizon), each `coherent`. For
each ablation ID:

```bash
eval $(opam env)
make ablationcheck TARGET=prod ABLATION=<ablation ID>
```

✅ `IDENTICAL` and `COHERENT` every time.

### B4. The record

The curve per task type:

```sql
SELECT decider, horizon_days, count(*) AS decisions, count_if(reproduced) AS reproduced,
       round(avg(same), 2) AS same, round(avg(agreement), 2) AS agreement
FROM pitlake_prod.control.horizon_curves
WHERE phase IN ('full', 'ladder')
GROUP BY decider, horizon_days
ORDER BY decider, horizon_days DESC NULLS FIRST
```

The truncation points and what they cost:

```sql
SELECT c.decider, c.decision_at, c.holds_to, c.breaks_at, t.worlds, t.sampled, t.minutes
FROM (SELECT DISTINCT point_key, decider, decision_at, holds_to, breaks_at
      FROM pitlake_prod.control.horizon_curves) c
JOIN (SELECT point_key, count(*) AS worlds, count_if(status = 'sampled') AS sampled,
             round(sum(seconds) / 60, 1) AS minutes
      FROM pitlake_prod.control.horizon_tests
      GROUP BY point_key) t ON t.point_key = c.point_key
ORDER BY c.decider, c.decision_at
```

Send Claude both results. **Stage 5 is complete** when B1 to B3 hold. Claude then records the
curves, the truncation points and what they say about each decider in ADR 0027.

# Counterfactual Stage 3 runbook: attribution

The `attribution` job decides at instants of a recorded backtest in the real world, in the null
world, and in a world without each item knowable there, one at a time, and records what each
removal changed: every strategy weight, and every settled rating of the agent with a p-value
([ADR 0025](../adr/0025-attribution.md)). A run stops starting worlds after `minutes` and the same
command continues it. Stage 3 is complete when every decision of the prod set is `ATTRIBUTED`, each
with a null world that changes nothing, and a world that changed a decision passes the independent
check.

Stage 2 must be complete first: the prod points of
[cf-stage2-decision-harness.md](cf-stage2-decision-harness.md) B1 to B4 reproducible. Follow the
parts in order. If you see something other than what a step says, stop and keep the full output.
Every time is UTC.

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
databricks bundle run attribution -t dev --params deciders=strategy,minutes=0.2
```

✅ `TERMINATED SUCCESS` with a line starting `PARTIAL: strategy at 2025-01-15 06:00 UTC`, then
`null world changes nothing`, a number of `worlds left` above 0, and last
`PARTIAL: 1 of 1 decisions still have worlds to attribute`. Dev's set is small enough that the
strategy can finish within the 12 seconds: `ATTRIBUTED` with `0 worlds left` and `OK` is also a
pass (run 1016487091203844 did, with 146 items). In dev every weight at 2025-01-15 is 0, since the
lookback needs more history than dev has, so no item changes a part.

### A3. The same command continues it

```bash
databricks bundle run attribution -t dev --params deciders=strategy
```

✅ `ATTRIBUTED: strategy at 2025-01-15 06:00 UTC` with `0 worlds left`, as many items
`evaluated` as are knowable (A2's included), one more world that `equal the full rebuild` than
items that `change at least one part` (the null world), and last `OK: 1 decisions in pitlake_dev
attributed, each null world unchanged`. It decides no world again: every one comes from A2's
records.

### A4. The agent

```bash
databricks bundle run attribution -t dev --params deciders=agent,samples=3,bars=1
```

A few minutes. ✅ `ATTRIBUTED: agent at 2025-01-15 06:00 UTC` with the ratings that settled, most
items `screened (inputs unchanged)`, a few `sampled`, `0 worlds left`, and
`null world changes nothing`. ❌ `FAILED` with `not reproducible`: no rating settled in the real
world; that is a finding, not a bug. Send Claude the output.

---

## Part B: prod and the Stage 3 exit test

The decision set is the strategy and the agent at three instants of backtest
`a4d4c4e0955046b8a6fb6a171f8b1071`: 2026-06-01 (Stage 2's earlier instant), 2026-08-27 (the day
after NVIDIA's 10-Q `filing:0001045810-26-000075` became public) and 2026-10-08 (its last).

### B1. The strategy

```bash
make deploy-prod
databricks bundle run attribution -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=strategy,at=2026-06-01 2026-08-27 2026-10-08"
```

About half an hour. ✅ Three `ATTRIBUTED` lines, each with `null world changes nothing` and
`rebuild equal`, and the items that changed a weight. If the last line is `PARTIAL`, run the same
command again until it is `OK`. Send Claude every run ID.

### B2. The agent

```bash
databricks bundle run attribution -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=agent,at=2026-06-01 2026-08-27 2026-10-08"
```

Up to two hours per run. Run the same command again while the last line is `PARTIAL`; each run
continues where the one before stopped. ✅ Three `ATTRIBUTED` lines and `OK: 3 decisions`. Send
Claude every run ID.

### B3. The independent check of a world that changed a decision

Take the first item listed under `items change at least one part` for the agent at 2026-10-08 (or,
if no item changed it, for the strategy), and withhold it with Stage 1's job:

```bash
databricks bundle run counterfactual -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,withhold=<item ID>"
```

✅ `COHERENT` for the null world and for the item. Then:

```sql
SELECT ablation_id, withheld, coherent
FROM pitlake_prod.control.ablations
WHERE run_id = (SELECT max_by(run_id, created_at) FROM pitlake_prod.control.ablations)
ORDER BY created_at
```

```bash
eval $(opam env)
make ablationcheck TARGET=prod ABLATION=<ablation ID of the item>
```

✅ `IDENTICAL` and `COHERENT`.

### B4. The record

```sql
SELECT p.decider, p.decision_at, p.settled, a.kind, a.status, count(*) AS worlds,
       count_if(size(a.caused) > 0) AS causing, count_if(a.rebuilt) AS rebuilt,
       round(sum(a.seconds) / 60, 1) AS minutes
FROM pitlake_prod.control.attribution_points p
JOIN pitlake_prod.control.attributions a ON a.point_key = p.point_key AND a.valid
GROUP BY ALL
ORDER BY p.decider, p.decision_at, a.kind, a.status
```

Send Claude the result. **Stage 3 is complete** when B1 to B3 hold. Claude then records the
attributions, the null worlds and the cost per decision in ADR 0025.

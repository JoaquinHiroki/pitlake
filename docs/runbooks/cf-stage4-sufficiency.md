# Counterfactual Stage 4 runbook: sufficiency

The `sufficiency` job searches, for each decision, for a minimal set of the items knowable at its
instant that still gives the same decision when every other item is withheld, then builds the
world from that set alone, decides again, checks it against a full rebuild and records it as an
ablation for the independent check ([ADR 0026](../adr/0026-sufficiency.md)). A run stops starting
worlds after `minutes` and the same command continues it. Stage 4 is complete when every decision
of the prod set is `SUFFICIENT` and the world of each set passes `make ablationcheck`.

Stage 2 must be complete first: the prod points of
[cf-stage2-decision-harness.md](cf-stage2-decision-harness.md) B1 to B4 reproducible. Stage 3 is
not needed; a decision it already recorded with the same `samples` and `min_agreement` is reused.
Follow the parts in order. If you see something other than what a step says, stop and keep the
full output. Every time is UTC.

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
databricks bundle run sufficiency -t dev --params deciders=strategy,minutes=0.2
```

✅ `TERMINATED SUCCESS` with a line starting `PARTIAL: strategy at 2025-01-15 06:00 UTC`, then
`search stopped after` a number of worlds, and last
`PARTIAL: 1 of 1 decisions still have worlds to try`.

### A3. The same command continues it

```bash
databricks bundle run sufficiency -t dev --params deciders=strategy
```

✅ `SUFFICIENT: strategy at 2025-01-15 06:00 UTC` with `are enough and none of them can go`
and the worlds tried (A2's included), then `the set alone decides the same`, `rebuild equal` and
`ablation <ID> coherent`, one line per entity of the set, and last
`OK: 1 decisions in pitlake_dev have a minimal sufficient set`. Keep the ablation ID.

### A4. The independent check of the set's world

```bash
eval $(opam env)
make ablationcheck TARGET=dev ABLATION=<ablation ID from A3>
```

✅ `IDENTICAL` and `COHERENT`.

### A5. The agent

```bash
databricks bundle run sufficiency -t dev --params deciders=agent,samples=3
```

Up to an hour. ✅ `SUFFICIENT: agent at 2025-01-15 06:00 UTC` with the ratings that settled, a
number of worlds `sampled` well below the worlds tried, `the set alone decides the same` and
`coherent`. If the last line is `PARTIAL`, run the same command again. ❌ `FAILED` with
`not reproducible`: no rating settled in the real world; that is a finding, not a bug. Send Claude
the output.

---

## Part B: prod and the Stage 4 exit test

The decision set is Stage 3's: the strategy and the agent at three instants of backtest
`a4d4c4e0955046b8a6fb6a171f8b1071`, 2026-06-01, 2026-08-27 and 2026-10-08.

### B1. The strategy

```bash
make deploy-prod
databricks bundle run sufficiency -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=strategy,at=2026-06-01 2026-08-27 2026-10-08"
```

About an hour. ✅ Three `SUFFICIENT` lines, each with only bars, 21 for each symbol with a weight
above zero and none for the others, `decides the same`, `rebuild equal` and `coherent`. If the last
line is `PARTIAL`, run the same command again until it is `OK`. Send Claude every run ID.

### B2. The agent

```bash
databricks bundle run sufficiency -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=agent,at=2026-06-01 2026-08-27 2026-10-08"
```

Up to two hours per run. Run the same command again while the last line is `PARTIAL`; each run
replays the worlds already tried and continues from the first one missing. ✅ Three `SUFFICIENT`
lines and `OK: 3 decisions`. Send Claude every run ID.

### B3. The independent check of every set's world

```sql
SELECT decider, decision_at, size(kept) AS kept, ablation_id, confirmed, rebuilt, coherent
FROM pitlake_prod.control.sufficient_sets
ORDER BY decider, decision_at
```

Six rows, each `confirmed`, `rebuilt` and `coherent`. For each ablation ID:

```bash
eval $(opam env)
make ablationcheck TARGET=prod ABLATION=<ablation ID>
```

✅ `IDENTICAL` and `COHERENT` six times.

### B4. The record

```sql
SELECT s.decider, s.decision_at, s.candidates, s.kept_by_kind, s.tests, s.sampled,
       count_if(t.stopped) AS stopped_early, round(sum(t.seconds) / 60, 1) AS minutes
FROM pitlake_prod.control.sufficient_sets s
JOIN pitlake_prod.control.sufficiency_tests t ON t.point_key = s.point_key
GROUP BY ALL
ORDER BY s.decider, s.decision_at
```

```sql
SELECT decider, decision_at, kept
FROM pitlake_prod.control.sufficient_sets
ORDER BY decider, decision_at
```

Send Claude both results. **Stage 4 is complete** when B1 to B3 hold. Claude then records the
sets, what they say each decider needs, and the cost per decision in ADR 0026.

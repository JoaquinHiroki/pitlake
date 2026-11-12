# Counterfactual Stage 6 runbook: monotonicity

The `monotonicity` job decides, for each decision, in worlds given a growing random share of the
items knowable at its instant, in several random orders, and scores every decision by the active
return of its tilt over the days after the instant. It then tests every pair of shares for a fall
or a rise in quality, with a sign-flip test and Holm's correction, records one world per decision
as an ablation for the independent check, and prints the curve and the verdict per decider
([ADR 0028](../adr/0028-monotonicity.md)). A run stops starting worlds after `minutes` and the same
command continues it. Stage 6 is complete when every decision of the prod set is `MEASURED`, each
decider has a `MONOTONIC` or `NON-MONOTONIC` verdict at 0.05, and every checked world passes
`make ablationcheck`.

Stage 2 must be complete first. Stages 3 to 5 are not needed. A decision is only scored once the
outcome window after it is over, so every instant must be at least `days` before the latest close
in Gold. Follow the parts in order. If you see something other than what a step says, stop and
keep the full output. Every time is UTC.

---

## Part A: dev

Dev Gold has closes from 2025-01-01 to 2025-01-14 only, so its last decision, 2025-01-15, has no
outcome yet. Part A decides at 2025-01-06 with a 3-day window instead.

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes and `Deployment complete!`.

### A2. A decision with no outcome yet fails

```bash
databricks bundle run monotonicity -t dev --params deciders=strategy,minutes=0
```

✅ The run **fails**: the task prints a line starting `FAILED: strategy at 2025-01-15 06:00 UTC`,
then `problem: the outcome is not known yet: no close of BTCUSDT became public after 2025-02-14
06:00 UTC`, and fails with `1 decisions failed monotonicity`. Nothing was decided.

### A3. The strategy, stopped early

```bash
databricks bundle run monotonicity -t dev --params "deciders=strategy,at=2025-01-06,days=3,minutes=0"
```

✅ `TERMINATED SUCCESS` with `PARTIAL: strategy at 2025-01-06 06:00 UTC`, its outcome
(`outcome to 2025-01-09 06:00 UTC:` and a return for AAPL, BTCUSDT and SPY; dev has no MSFT or
NVDA closes, so they are left out of the score),
`0 worlds decided`, `stopped before every world was decided`, and last
`PARTIAL: 1 of 1 decisions still have worlds to decide`.

### A4. The same command continues it

```bash
databricks bundle run monotonicity -t dev --params "deciders=strategy,at=2025-01-06,days=3"
```

Under 15 minutes. ✅ `MEASURED: strategy at 2025-01-06 06:00 UTC` with about `47 worlds decided`
(fewer if two shares of a small set keep the same items), one
line per share from `0%` to `100%`, each quality `+0.0000` (in dev every weight is 0, as Stage 3's
runbook says, so every tilt scores 0), `checked: ablation <ID> coherent`, then a
`QUALITY: strategy over 1 decisions, 5 chains` block with `MONOTONIC at 0.05 (Holm over 55 pairs)`,
and last `OK: 1 decisions in pitlake_dev have a quality curve; strategy MONOTONIC`. Keep the
ablation ID.

### A5. The independent check of the checked world

```bash
eval $(opam env)
make ablationcheck TARGET=dev ABLATION=<ablation ID from A4>
```

✅ `IDENTICAL` and `COHERENT`.

### A6. The agent

```bash
databricks bundle run monotonicity -t dev --params "deciders=agent,at=2025-01-06,days=3,levels=0 0.5 1,draws=2,samples=3"
```

Under half an hour. ✅ `MEASURED: agent at 2025-01-06 06:00 UTC` with `4 worlds decided`,
`4 sampled`, three share lines, `checked: ablation <ID> coherent`, a
`QUALITY: agent over 1 decisions, 2 chains` block with either verdict (two chains cannot show a
significant fall), and last `OK`. If the last line is `PARTIAL`, run the same command again. Send
Claude the output.

---

## Part B: prod and the Stage 6 exit test

The decision set is Stage 3's: the strategy and the agent at three instants of backtest
`a4d4c4e0955046b8a6fb6a171f8b1071`, 2026-06-01, 2026-08-27 and 2026-10-08, each scored over the
30 days after it. The last window ends 2026-11-07 06:00, so prod Gold must hold closes of every
symbol public after that, which the daily ingest provides.

### B1. The strategy

```bash
make deploy-prod
databricks bundle run monotonicity -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=strategy,at=2026-06-01 2026-08-27 2026-10-08"
```

Under an hour. ✅ Three `MEASURED` lines, each with its outcome, `47 worlds decided`, eleven share
lines with `0%` at `+0.0000`, and `checked: ablation <ID> coherent`; then the
`QUALITY: strategy over 3 decisions, 15 chains` block with `MONOTONIC` or `NON-MONOTONIC` at 0.05,
a `stops helping at` line and a `0% to 100%` line; and last `OK: 3 decisions`. If the last line is
`PARTIAL`, run the same command again until it is `OK`. ❌ `FAILED` with
`the outcome is not known yet`: the ingest is behind; check the latest close as in B4 and send
Claude the output. Send Claude every run ID and the output.

### B2. The agent

```bash
databricks bundle run monotonicity -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=agent,at=2026-06-01 2026-08-27 2026-10-08"
```

Up to two hours per run, and two or three runs: 141 worlds of 10 samples each. Run the same
command again while the last line is `PARTIAL`; each run reuses the worlds already decided. ✅ Three
`MEASURED` lines, the `QUALITY: agent over 3 decisions, 15 chains` block with a verdict, and
`OK: 3 decisions`. Send Claude every run ID and the output.

### B3. The independent check of every checked world

```sql
SELECT DISTINCT decider, decision_at, ablation_id, coherent
FROM pitlake_prod.control.monotonicity_curves
WHERE ablation_id IS NOT NULL
ORDER BY decider, decision_at
```

Six rows, each `coherent`. For each ablation ID:

```bash
eval $(opam env)
make ablationcheck TARGET=prod ABLATION=<ablation ID>
```

✅ `IDENTICAL` and `COHERENT` every time.

### B4. The record

The curve per decider:

```sql
SELECT decider, fraction, count(*) AS chains, round(avg(quality), 4) AS quality,
       round(min(quality), 4) AS lowest, round(max(quality), 4) AS highest
FROM pitlake_prod.control.monotonicity_curves
GROUP BY decider, fraction
ORDER BY decider, fraction
```

The pairs that fall or rise after adjustment, and the smallest share to the largest:

```sql
SELECT decider, low, high, round(change, 4) AS change, round(p_falls, 4) AS p_falls,
       round(holm_falls, 4) AS holm_falls, round(p_rises, 4) AS p_rises,
       round(holm_rises, 4) AS holm_rises
FROM pitlake_prod.control.monotonicity_pairs
WHERE falls OR rises OR (low = 0 AND high = 1)
ORDER BY decider, falls DESC, low, high
```

What the worlds cost:

```sql
SELECT c.decider, c.decision_at, count(*) AS worlds, count_if(t.status = 'sampled') AS sampled,
       round(sum(t.seconds) / 60, 1) AS minutes
FROM (SELECT DISTINCT point_key, decider, decision_at, test_id
      FROM pitlake_prod.control.monotonicity_curves) c
JOIN pitlake_prod.control.monotonicity_tests t ON t.test_id = c.test_id
GROUP BY c.decider, c.decision_at
ORDER BY c.decider, c.decision_at
```

If B1 failed on the outcome, the latest close per symbol:

```sql
SELECT entity, max(knowable_at) AS latest
FROM pitlake_prod.gold.features
WHERE feature = 'close'
GROUP BY entity
ORDER BY entity
```

Send Claude the results. **Stage 6 is complete** when B1 to B3 hold. Claude then records the
curves, the verdicts, where information stops helping and what they say about each decider in
ADR 0028.

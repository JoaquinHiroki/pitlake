# Counterfactual Stage 7 runbook: decomposition

The `decomposition` job asks an endpoint, with no tools, for closes it could remember, then at the
1st of every month of a recorded backtest decides in three conditions: blind (no tools, no date),
recall (no tools, the date) and tooled (the time-locked agent on the real world). It scores each
by the active return of its tilt over the days after the instant and decomposes the tooled agent's
skill into prior, memorised and inferred components either side of the endpoint's training cutoff,
with bootstrap intervals over instants ([ADR 0029](../adr/0029-decomposition.md)). A run stops
starting decisions after `minutes` and the same command continues it. Stage 7 is complete when
Maverick's decomposition in prod gives the memorised and inferred components each with a 95%
interval.

Stage 2 must be complete first. Stages 3 to 6 are not needed. No endpoint remembers 2025 (ADR
0029), so prod needs prices from 2024 before Part B can decompose anything: B1 backfills them.
Follow the parts in order. If you see something other than what a step says, stop and keep the
full output. Every time is UTC.

---

## Part A: dev

Dev Gold has BTCUSDT closes to 2025-01-14 but AAPL and SPY closes only from 2025-01-02 to the
session of 2025-01-10, public at 05:00 on 01-11, so Part A decides at two instants with a 2-day
window and a cutoff placed between them. It checks the job, not the model: Maverick does not
remember January 2025.

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes and `Deployment complete!`.

### A2. Maverick on dev

```bash
databricks bundle run decomposition -t dev --params "endpoint=databricks-llama-4-maverick,cutoff=2025-01-06,at=2025-01-03 2025-01-06,days=2,samples=3"
```

Under 15 minutes. ✅ `TERMINATED SUCCESS` with, in order:

- a `RECALL: databricks-llama-4-maverick asked with no tools ...` block with one line per instant
  and window end (2025-01-03, 01-05, 01-06 and 01-08), each listing AAPL, BTCUSDT and SPY with the
  close it gave and the real one, then `before the cutoff: <n> of 2 times recalled` and
  `after the cutoff: <n> of 2 times recalled`. Maverick says every date of 2025 is in the future,
  so on dev each close is `no answer` and both counts are 0;
- `DECOMPOSED: 2025-01-03 06:00 UTC, before the cutoff; outcome to 2025-01-05 06:00 UTC: ...` and
  `DECOMPOSED: 2025-01-06 06:00 UTC, after the cutoff; outcome to 2025-01-08 06:00 UTC: ...`, each followed by
  `quality: blind ..., recall ..., tooled ...` and nothing after it: a line ending
  `(<n> samples gave no valid answer and score 0)` means Maverick did not answer through its
  `decide` tool (ADR 0029), so stop and send Claude the output;
- a `DECOMPOSITION: databricks-llama-4-maverick, cutoff 2025-01-06, 1 instants before it and 1
  after (0 straddling it left out)` block in which every value says `(no interval)`, since one
  instant per era cannot have one;
- last `OK: 2 instants decomposed in pitlake_dev; memorised ..., inferred ...`.

If the last line is `PARTIAL`, run the same command again. Send Claude the output.

### A3. The same command again reuses everything

Run A2's command again.

Under 5 minutes. ✅ The same lines with the same numbers. Then in **SQL Editor**:

```sql
SELECT run_id, count(*) AS decided
FROM pitlake_dev.control.decomposition_tests
GROUP BY run_id
ORDER BY min(created_at)
```

✅ Six decisions in all, and no row with A3's run ID: A3 decided nothing new. They are one blind,
two recall and three tooled, one of which is the invalid tooled decision of run 377456453323593
(2026-11-13), made before Maverick answered by tool, under a key no later run uses. A2 may itself
have reused the blind and recall decisions that run made at 2025-01-03.

---

## Part B: prod and the Stage 7 exit test

### B1. Prices from 2024

`collector/config.prod.toml` starts `binance` `spot_trades`, `coinbase` `spot_candles_1m`,
`alpaca` `stock_trades` and `alpaca` `stock_bars_1d` on 2024-01-01, as FRED and EDGAR already did,
so prod's next ingest collects 2024 for them:

```bash
make deploy-prod
databricks bundle run ingest -t prod --params max_files=120
```

Repeat the ingest, at most a few times a day, until a run's `load_bronze` iterations all print
`0 files to load`, as in the [deploy runbook](deploy.md) D2. Each commit starts
`pitlake-transform-prod` by itself.

❌ A run that fails with a compute-quota message: stop for the day and repeat tomorrow.
❌ A transform whose `gate` prints `BREACH` for a day of 2024: send Claude the `BREACH` lines.
Waive a day only once its cause is understood, as the
[Stage 4 runbook](stage4-quality-gate.md) B2 does.

When the ingest has caught up, in **SQL Editor**:

```sql
SELECT entity, min(event_time) AS first, max(event_time) AS last, count(*) AS closes
FROM pitlake_prod.gold.features
WHERE feature = 'close'
GROUP BY entity
ORDER BY entity
```

✅ Five rows, each `first` on 2024-01-02 or earlier (BTCUSDT 2024-01-01) and `last` within the
last few days. Send Claude the result.

### B2. A backtest from 2024

```bash
databricks bundle run research -t prod --params start=2024-02-01
```

✅ `TERMINATED SUCCESS` and 24 backtests from 2024-02-01. Copy the research **run ID**.

### B3. Maverick

```bash
databricks bundle run decomposition -t prod --params "endpoint=databricks-llama-4-maverick,cutoff=2024-09-01,backtest=<research run ID from B2>"
```

About an hour, one or two runs. ✅ In order:

- the `RECALL` block, one line per instant and window end from 2024-02-01 to 2026-10-31, with most
  times before 2024-09-01 `recalled` and few after;
- `LEFT OUT` only for the last month or two, `the outcome is not known yet`, and no instant
  straddling the cutoff;
- one `DECOMPOSED` line per month from 2024-02-01, 33 to 2026-10-01: 7 `before the cutoff`
  (2024-02-01 to 2024-08-01) and the rest `after`;
- the `DECOMPOSITION` block, `7 instants before it and 26 after` (more after if run later), with
  an interval on every value, then the `memorised`, `inferred` and `tools over memory` lines;
- last `OK: 33 instants decomposed in pitlake_prod; memorised ..., inferred ...`.

If the last line is `PARTIAL`, run the same command again until it is `OK`. Send Claude every run
ID and the output.

### B4. Llama 3.3, the control

```bash
databricks bundle run decomposition -t prod --params "endpoint=databricks-meta-llama-3-3-70b-instruct,cutoff=2024-01-01,backtest=<research run ID from B2>"
```

About an hour. ✅ A `RECALL` block with few times recalled (`after the cutoff: <n> of <m>`, no
times before it), B3's `DECOMPOSED` instants all `after the cutoff`, a `DECOMPOSITION` block with
`0 instants before it and 33 after` in which the before column and `memorised` are `n/a`, and last
`OK`. Send Claude every run ID and the output.

### B5. The record

The components per endpoint:

```sql
SELECT endpoint, component, era, instants, round(estimate, 4) AS estimate, round(low, 4) AS low,
       round(high, 4) AS high
FROM pitlake_prod.control.decompositions
WHERE run_id IN (SELECT max_by(run_id, created_at) FROM pitlake_prod.control.decompositions
                 GROUP BY endpoint)
ORDER BY endpoint, era, component
```

The quality per instant:

```sql
SELECT endpoint, decision_at, era, round(blind, 4) AS blind, round(recall, 4) AS recall,
       round(tooled, 4) AS tooled, round(recall_error, 3) AS recall_error
FROM pitlake_prod.control.decomposition_scores
WHERE run_id IN (SELECT max_by(run_id, created_at) FROM pitlake_prod.control.decomposition_scores
                 GROUP BY endpoint)
ORDER BY endpoint, decision_at
```

What the decisions cost:

```sql
SELECT endpoint, condition, count(*) AS decisions, sum(samples) AS samples,
       sum(invalid) AS invalid, round(sum(seconds) / 60, 1) AS minutes
FROM pitlake_prod.control.decomposition_tests
WHERE valid
GROUP BY endpoint, condition
ORDER BY endpoint, condition
```

Send Claude the results. **Stage 7 is complete** when B3 gives the memorised and inferred
components each with an interval and B4 holds. Claude then records the probe, the components, the
control and what they say about the agent in ADR 0029.

# Stage 4 runbook: cross-venue comparison and the quality gate

The transform job now runs `plan → transform → gate → commit`. The gate compares Binance and
Coinbase minute by minute and checks rejection rates against `control.quality_thresholds`. If any
check fails without a waiver, the run fails and nothing is committed
([ADR 0007](../adr/0007-cross-venue-quality-gate.md)).

Follow the parts in order. If you see something other than what a step says, stop and keep the
full output.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make validate
make deploy
```

✅ `258 passed, 2 skipped`, `Validation OK!`, `Deployment complete!`. In **Jobs & Pipelines**,
`pitlake-transform-dev` shows `plan → transform → gate → commit`.

### A2. Run it

```bash
make transform
```

✅ `TERMINATED SUCCESS`. The `gate` task prints `BTC: 14 days to compare`,
`BTC: wrote the comparison of 14 days` and `OK: quality gate passed 28 checks in pitlake_dev`.

### A3. Rehearse the exit test in dev

```bash
databricks bundle run transform -t dev --params period_start=2025-01-02,period_end=2025-01-03,inject_fault=venue_price_divergence
```

✅ It ends **`TERMINATED FAILED`**: `gate` fails with
`GateFailure: quality gate failed on 2 checks: venue_divergence BTC 2025-01-02: 60 of 1440 ...`
and `commit` shows *Upstream failed*.

---

## Part B: prod

### B1. Deploy and run

```bash
make deploy-prod
databricks bundle run transform -t prod
```

The first gate compares every day since 2025-01-01 and **is expected to fail** on three days, which
it prints as `BREACH` lines:

```
BREACH: venue_divergence BTC 2025-10-10: 13 of 1440
BREACH: venue_missing BTC 2025-10-25: 361 of 1440
BREACH: venue_missing BTC 2026-05-08: 402 of 1440
```

❌ If other days appear, send Claude the `BREACH` lines before going on.

### B2. Record the three waivers

These were investigated in ADR 0007. In **SQL Editor**:

```sql
INSERT INTO pitlake_prod.control.quality_waivers VALUES
  ('venue_divergence', 'BTC', DATE'2025-10-10',
   'Crash and liquidation cascade of 10 October 2025: closes up to 454 bps apart in 13 minutes. A market event on both venues, not an ingestion fault.',
   current_user(), current_timestamp()),
  ('venue_missing', 'BTC', DATE'2025-10-25',
   'Coinbase served no BTC-USD candles for 349 consecutive minutes while Binance traded normally. Coinbase outage; the gap is recorded in silver.sequence_gaps.',
   current_user(), current_timestamp()),
  ('venue_missing', 'BTC', DATE'2026-05-08',
   'Coinbase served no BTC-USD candles for 391 consecutive minutes while Binance traded normally. Coinbase outage; the gap is recorded in silver.sequence_gaps.',
   current_user(), current_timestamp());
```

### B3. Run again

```bash
databricks bundle run transform -t prod
```

✅ `TERMINATED SUCCESS`. The gate prints the three days as `WAIVED (...)`, then
`BTC: wrote the comparison of <n> days`, and `commit` lists `cross_venue_1m`.

From now on the gate runs after every daily ingest. A failure email whose `gate` output says
`BREACH` means a new day failed a check. Send Claude the `BREACH` lines, and only after the cause is
understood, add a waiver for that day as in B2.

---

## Part C: the Stage 4 exit test

### C1. Inject discrepancies into prod

```bash
databricks bundle run transform -t prod --params period_start=2026-09-01,period_end=2026-09-07,inject_fault=venue_price_divergence
databricks bundle run transform -t prod --params period_start=2026-09-01,period_end=2026-09-07,inject_fault=venue_missing_minutes
```

✅ Both end **`TERMINATED FAILED`**. That is the expected result: `gate` fails with
`GateFailure: quality gate failed on 7 checks` (one per day), and `commit` shows *Upstream failed*.

### C2. Check that nothing reached the research layer

Tell Claude the two runs are done. Claude checks that:

- `control.quality_gate_results` records 7 failed checks per run, `venue_divergence` for the first
  and `venue_missing` for the second, each with its `injected_fault`
- `control.silver_commits` is unchanged
- `silver.cross_venue_1m` still shows every minute of 2026-09-01..07 as it was, with no injected
  divergence

**Stage 4 is complete** when all three hold.

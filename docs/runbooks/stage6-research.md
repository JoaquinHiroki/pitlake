# Stage 6 runbook: research, experiment tracking and reproducible backtests

The research job runs on demand. It records the committed Gold version it reads, runs 24 backtests,
and logs each to MLflow and `control.backtest_runs`. With `replay` set, it reproduces them from the
recorded versions and fails unless every one is identical
([ADR 0009](../adr/0009-research-and-reproducible-backtests.md)).

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

✅ **Jobs & Pipelines** shows `pitlake-research-dev`, and **Experiments** shows
`pitlake-backtests-dev`.

### A2. Apply the Gold retention

```bash
make transform
```

✅ `TERMINATED SUCCESS`. The `plan` task prints `gold.<table>: versions now readable for interval
365 days` for the four Gold tables (only on this first run).

### A3. Run the backtests

Dev holds only two weeks of January 2025, so start there:

```bash
databricks bundle run research -t dev --params start=2025-01-02
```

✅ `TERMINATED SUCCESS`. The task prints the Gold version it read, one line per backtest, and
`OK: ran 24 backtests in pitlake_dev`. In dev every backtest stays flat (20-day volatility needs
more history than dev has), which is fine: this step proves the plumbing. Copy the **run ID** from
the Run URL (`.../runs/<run ID>`).

### A4. Replay them

```bash
databricks bundle run research -t dev --params replay=<run ID from A3>
```

✅ `TERMINATED SUCCESS`, 24 lines starting with `IDENTICAL`, and `OK: reproduced 24 backtests`.

---

## Part B: prod

### B1. Deploy, apply the retention, run the backtests

```bash
make deploy-prod
databricks bundle run transform -t prod
databricks bundle run research -t prod
```

✅ All three end `TERMINATED SUCCESS`. The transform's `plan` prints the retention lines, and the
research job prints 24 backtests from 2025-02-01. Send Claude the research **run ID**.

---

## Part C: the Stage 6 exit test

### C1. Move Gold forward

```bash
databricks bundle run transform -t prod
```

✅ `TERMINATED SUCCESS` and `committed 4 Gold tables`: `gold.features` now has a newer committed
version than the one the backtests read.

### C2. Reproduce the backtests from their recorded versions

```bash
databricks bundle run research -t prod --params replay=<run ID from B1>
```

✅ `TERMINATED SUCCESS`: it reads the **recorded** (older) Gold version, prints 24 `IDENTICAL`
lines, and `OK: reproduced 24 backtests`. Tell Claude the run ID. Claude checks that every replay
row in `control.backtest_runs` has `reproduced = true` and the same hashes as its original, that the
version read is older than the latest Gold commit, and that MLflow shows the replays next to the
originals.

**Stage 6 is complete** when C2 holds.

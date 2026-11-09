# Counterfactual Stage 2 runbook: the decision harness

The `harness` job decides at a recorded backtest's instant, in the real world or one with items
withheld, with the backtest strategy and the time-locked agent, samples each in rounds, and fails
unless every decision is the same in every round and enough samples agree
([ADR 0024](../adr/0024-decision-harness.md)). Stage 2 is complete when that holds in prod for the
real world, a world with items withheld and an earlier instant, and a replay in a later run gives
the same decisions.

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

### A2. Decide in dev

```bash
databricks bundle run harness -t dev --params samples=3
```

A few minutes. ✅ `TERMINATED SUCCESS` with two lines starting `REPRODUCIBLE`:

- `strategy at 2025-01-15 06:00 UTC, withheld nothing`: one decision in 2 of 2 rounds, `6 of 6
  samples agree (1.00 ...)`, and `inputs from the pure-Python as-of are equal to the recorded hash`;
- `agent at 2025-01-15 06:00 UTC, withheld nothing`: a decision such as
  `{"AAPL":"neutral",...}`, its agreement, the agreement per symbol, `0 invalid`, and about 10
  seconds and 6 to 7 model calls per sample.

Then `OK: 2 decision points in pitlake_dev, each reproducible`.

❌ `UNSTABLE: agent ...`: the rounds disagree or fewer than 80% of samples agree. That is a finding,
not a bug. Send Claude the output.

---

## Part B: prod and the Stage 2 exit test

All on backtest `a4d4c4e0955046b8a6fb6a171f8b1071` (lookback 20, target volatility 10%, the first
of research run 238695896043288).

### B1. The real world at the last instant

```bash
make deploy-prod
databricks bundle run harness -t prod --params backtest=a4d4c4e0955046b8a6fb6a171f8b1071
```

✅ `TERMINATED SUCCESS`: the strategy at 2026-10-08 06:00 with `10 of 10 samples agree (1.00`
and inputs `equal to the recorded hash`, and the agent `REPRODUCIBLE`. Send Claude the run ID.

### B2. A world with items withheld

```bash
databricks bundle run harness -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,withhold=filing:0001045810-26-000075 bar:AAPL/2026-06-15"
```

✅ Both `REPRODUCIBLE`, on a world hash other than B1's.

### B3. An earlier instant

```bash
databricks bundle run harness -t prod --params backtest=a4d4c4e0955046b8a6fb6a171f8b1071,at=2026-06-01
```

✅ Both `REPRODUCIBLE` at 2026-06-01 06:00 UTC.

### B4. Replay B1 in a new run

```bash
databricks bundle run harness -t prod --params replay=<run ID of B1>
```

✅ Both points `REPRODUCIBLE` with `replay of ...: IDENTICAL`. Send Claude the four run IDs.

**Stage 2 is complete** when B1 to B4 hold. Claude then records the agreement of each point and the
cost per sample from `control.decision_points` in ADR 0024.

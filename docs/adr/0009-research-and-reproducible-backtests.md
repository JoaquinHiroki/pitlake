# ADR 0009: Research job and reproducible backtests

Status: accepted (Stage 6). Builds on [ADR 0008](0008-gold-and-point-in-time-access.md).

## Context

Spec section 7 defines a Research job, run on demand, that records the exact version of every table
it reads, builds features, runs backtests across parameter sets, and logs every result. Section 9
has MLflow record every backtest with its parameters, results, code version and the table versions
it read. Stage 6 is complete when a backtest reproduced from its recorded versions returns identical
results. Section 11 makes profitability an explicit non-goal: the backtester exists to demonstrate
reproducibility.

## Decisions

**One job, two modes.** `pitlake-research-<target>` has one task, `backtest`, with no schedule.
Without parameters it runs the parameter grid on the latest committed Gold. With `replay` set to a
backtest ID or a research run ID, it reproduces those backtests from their recorded versions and
fails unless every one is identical. It neither retries nor lets serverless retry it: a backtest is
deterministic, and a retry would only log it twice.

**What a backtest reads, and how.** The job takes the latest row of `control.gold_commits` for
`gold.features`, resolves the version Delta can read (`pitlake.snapshots`), and records that
version. It asks `pitlake.pit.as_of` for `close`, `volatility_20d`, `net_income_q` and `cpi_yoy` at
06:00 UTC on every day of the period, for BTCUSDT, SPY, AAPL, MSFT and NVDA. At 06:00 UTC the
previous crypto day and the previous New York session are both knowable. Nothing else is read:
realized returns come from the closes `as_of` returned at consecutive decisions, so the evaluation
cannot see a price before the strategy could have.

**The backtest is pure Python.** Once the inputs are collected (about 13,000 values), the strategy
runs in `pitlake.backtest` without Spark, in a fixed order, so the same inputs give the same floats.
It is tested on every change without a cluster.

**A deliberately small strategy.** Each asset is held long when its close is above the close
`lookback` observations earlier, sized to `target_vol` given its 20-day volatility (capped at 2x),
with the five assets sharing the book equally. `require_profit` keeps a company flat unless its
latest quarterly net income, as known at the time, is positive (which uses the fundamentals join
chain). `macro_brake` halves exposure while year-over-year CPI, as known at the time, is above 3%
(which uses the macro vintages). Turnover costs 5 bps. The grid is lookback 20, 60 or 120, target
volatility 10% or 20%, and both filters on or off: 24 backtests.

**Every backtest is recorded twice.** In MLflow, under a parent run per research run in the
experiment `/Users/<user>/pitlake-backtests-<target>` (created by the bundle), each backtest logs
its parameters, metrics, daily results (`daily.json`), and tags: the Gold features version, the Gold
commit, the Silver versions that Gold was built from, the git commit the bundle was deployed from,
the wheel version, and two hashes. In `control.backtest_runs`, the same facts form one row per
backtest, which is what a replay and the Stage 7 console read.

**Reproduction is judged by hashes.** The input hash is a SHA-256 of every value `as_of` returned,
with its entity, instant, feature and event time. The result hash covers every day's return, cost,
exposure and turnover, and the metrics. A replay reads the recorded version, rebuilds the inputs,
reruns the recorded parameters, and compares both hashes and every metric exactly. It logs its
own MLflow run and `backtest_runs` row with `replay_of` and `reproduced`, and the job fails if any
backtest differs.

**Gold versions are kept for a year.** `gold.features` is rewritten by every transform run, and Delta
refuses to read a version whose files are older than the retention, 7 days by default. Every Gold
table now keeps deleted files and log entries for 365 days (`delta.deletedFileRetentionDuration`,
`delta.logRetentionDuration`), set in the DDL and applied to existing tables by the transform's
`plan` task. The features table holds about 140,000 rows, so a year of daily versions stays small.

## Consequences

- A backtest is reproducible for a year after the Gold version it read. After that, its record
  remains, but the replay fails with an error from Delta rather than a different result.
- Because `as_of` never returns a value that became knowable after the instant asked for, newer
  Gold versions usually give the same inputs for a past period. They differ when history itself
  changes, such as a Silver reprocess or a new quality rule. Recording the version is what makes the
  replay exact in those cases too.
- The code is recorded, not pinned. A replay runs with the code deployed at the time, and it reports
  when the recorded commit differs. A change to the strategy then shows up as a failed replay, which
  is the honest result.
- `require_profit` has had no effect so far: each pair of backtests that differs only in it has the
  same result to the last digit. Prices start in 2025, and no company in the universe reported a
  quarterly loss that was the latest known one at any decision since then. The last were MSFT's
  quarter to 2017-12-31 and NVDA's in 2008 and 2009. `tests/test_backtest.py` shows that a loss
  keeps the company flat. The flag keeps its meaning, since replays depend on it, and will
  separate the pairs once a loss is reported or the history reaches back to one.

## Exit test (prod, 2026-10-14)

Research run 238695896043288 (2026-10-13) ran the 24 backtests on `gold.features` version 4 and
recorded them in MLflow and `control.backtest_runs`. Scheduled transforms then moved the features
to version 6. Replay run 854075769456328 read version 4 again, not the latest, and reproduced all 24
backtests: 24 of 24 have `reproduced = true`, every input and result hash equals its original, and
MLflow holds a replay run for each. Stage 6 is complete.

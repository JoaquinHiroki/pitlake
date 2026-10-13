# Stage 5 runbook: Gold, point-in-time access and the leakage audit

The transform job now runs `plan → transform → gate → commit → gold → commit_gold`. `gold` builds
the bars, fundamentals and features from the committed Silver versions, audits them for leakage,
and writes them only if the audit finds no case
([ADR 0008](../adr/0008-gold-and-point-in-time-access.md)).

Follow the parts in order. If you see something other than what a step says, stop and keep the
full output.

---

## Part A: dev

### A1. CI

Push the commit and wait for **CI** on GitHub to pass. Its *Spark tests* step runs the leakage
suite (`tests/spark/test_leakage.py`), which cannot run on your machine. Tell Claude if it fails.

### A2. Test and deploy

```bash
make check
make validate
make deploy
```

✅ `pitlake-transform-dev` in **Jobs & Pipelines** shows
`plan → transform → gate → commit → gold → commit_gold`.

### A3. Run it

```bash
make transform
```

✅ `TERMINATED SUCCESS`. The `gold` task prints, among others:

```
binance: 14 files to compress into minute bars
...
ok: filing_invisible_before_acceptance fundamentals: 0 cases in <n> rows
ok: counterfactual <instant>: 0 cases in <n> rows
...
OK: Gold built and audited in pitlake_dev: no leakage in <n> checks
```

and `commit_gold` prints `OK: run <id> committed 4 Gold tables in pitlake_dev`.

### A4. Rehearse the exit test drill

```bash
databricks bundle run transform -t dev --params inject_fault=fundamentals_dated_by_period
```

✅ It ends **`TERMINATED FAILED`**: `gold` fails with
`LeakageFound: leakage audit found <n> cases: feature_after_source fundamentals: ...` and
`commit_gold` shows *Upstream failed*. Tell Claude the run is done.

---

## Part B: prod

### B1. Deploy and build Gold

```bash
make deploy-prod
databricks bundle run transform -t prod
```

The first run compresses every Silver file into minute bars: 645 Binance days (about 2.4 billion
trades), 645 Coinbase days and 1,756 Alpaca files. Expect it to take longer than the daily runs.

✅ `TERMINATED SUCCESS`, with `no leakage in <n> checks` and `committed 4 Gold tables`. Tell Claude
the run ID.

---

## Part C: the Stage 5 exit test

### C1. The leakage suite finds zero cases

CI for the Stage 5 commit passed (A1), and the prod audit of B1 recorded zero cases. Claude checks
`control.leakage_checks` for that run.

### C2. A company's results are invisible before their filing

Claude checks `control.filing_visibility` for the B1 run: every filing behind a fundamental feature
has `features_before = 0` (nothing from it visible one microsecond before acceptance) and
`features_at > 0` (visible at acceptance). `lead_days` shows how many days earlier a dataset dated
by period end would have shown each result.

### C3. The audit stops a leak in prod

```bash
databricks bundle run transform -t prod --params inject_fault=fundamentals_dated_by_period
```

✅ It ends **`TERMINATED FAILED`**: `gold` fails with `LeakageFound`, and `commit_gold` shows
*Upstream failed*. Claude checks that `control.gold_commits` did not change and that the Gold tables
have no version written by this run.

**Stage 5 is complete** when C1, C2 and C3 hold.

# Stage 3 runbook: Silver

The `pitlake-transform-<target>` job cleans the committed Bronze versions into Silver: it types
every column, assigns knowable_at, quarantines rejected rows and halts on a halt rule
([ADR 0006](../adr/0006-silver-quality-rules.md)). It has three tasks:
`plan → transform (one iteration per group of tables) → commit`.

Follow the parts in order. Each step says what you should see. If you see something else, stop and
keep the full output; don't continue to the next step.

- **Part A** (dev, about 30 minutes): deploy, build Silver from the dev sample, check that it adds up.
- **Part B** (prod, spread over a few days because of compute quota): build Silver for everything
  in Bronze.
- **Part C**: the Stage 3 exit test, which measures the rejection rates, explains them, and proves
  that the timestamp rules halt the run.

Run every command from `~/code/PITLake`. SQL goes in **SQL Editor** on the serverless starter
warehouse.

---

## Part A: dev

### A1. Log in, lint and test

```bash
databricks auth login --host https://dbc-cc51bd40-0056.cloud.databricks.com --profile pitlake
make check
```

✅ `Profile pitlake was successfully saved`, then `237 passed, 1 skipped`. The skipped test is the
Spark suite, which runs in GitHub Actions (it needs pyspark, which is not installed locally).

### A2. Deploy to dev

```bash
make validate
make deploy
```

✅ `Validation OK!`, then `Deployment complete!`. In **Jobs & Pipelines**, `pitlake-transform-dev`
shows `plan → transform → commit`, and its trigger is *Paused* (dev is run by hand).

❌ If validation rejects `table_update`, send Claude the error. The fallback is an hourly
`periodic` trigger.

### A3. Build Silver from the dev sample

```bash
make transform
```

It prints a **Run URL** and waits (about 5 to 10 minutes). ✅ `TERMINATED SUCCESS`, and:

- `plan`: `OK: 60 rules, 7 Silver tables in pitlake_dev: [...]`
- each `transform` iteration: one line per table such as
  `OK: binance.spot_trades transformed 14 files: 49675597 rows in, ... to Silver, rejected {...}, ... sequence gaps`
- `commit`: `OK: run <id> committed 9 Silver tables in pitlake_dev`

❌ If an iteration fails with `QualityHalt`, real dev data broke a halt rule. That is not a bug in
the run; it is the rule doing its job. Run this and send Claude the result:

```sql
SELECT * FROM pitlake_dev.control.quality_halts ORDER BY halted_at DESC;
```

❌ If a table says `no committed Bronze version yet`, run `make ingest` first, then repeat A3.

### A4. Check that Silver adds up

Every Bronze row must be in Silver or in the quarantine, and nowhere twice:

```sql
WITH latest AS (
  SELECT * FROM pitlake_dev.control.silver_load_log
  QUALIFY row_number() OVER (PARTITION BY silver_table, source_file ORDER BY processed_at DESC) = 1
), logged AS (
  SELECT silver_table, count(*) AS files, sum(rows_in) AS judged, sum(rows_out) AS kept,
         sum(rows_quarantined) AS quarantined
  FROM latest GROUP BY silver_table
), quarantine AS (
  SELECT silver_table, count(*) AS quarantine_rows FROM pitlake_dev.silver.quarantine GROUP BY ALL
), tables AS (
  SELECT 'binance_spot_trades' AS silver_table,
         (SELECT count(*) FROM pitlake_dev.bronze.binance_spot_trades) AS bronze_rows,
         (SELECT count(*) FROM pitlake_dev.silver.binance_spot_trades) AS silver_rows
  UNION ALL SELECT 'coinbase_spot_candles_1m',
         (SELECT count(*) FROM pitlake_dev.bronze.coinbase_spot_candles_1m),
         (SELECT count(*) FROM pitlake_dev.silver.coinbase_spot_candles_1m)
  UNION ALL SELECT 'fred_series_vintages',
         (SELECT count(*) FROM pitlake_dev.bronze.fred_series_vintages),
         (SELECT count(*) FROM pitlake_dev.silver.fred_series_vintages)
  UNION ALL SELECT 'alpaca_stock_trades',
         (SELECT count(*) FROM pitlake_dev.bronze.alpaca_stock_trades),
         (SELECT count(*) FROM pitlake_dev.silver.alpaca_stock_trades)
  UNION ALL SELECT 'alpaca_stock_bars_1d',
         (SELECT count(*) FROM pitlake_dev.bronze.alpaca_stock_bars_1d),
         (SELECT count(*) FROM pitlake_dev.silver.alpaca_stock_bars_1d)
  UNION ALL SELECT 'edgar_filings',
         (SELECT count(*) FROM pitlake_dev.bronze.edgar_filings),
         (SELECT count(*) FROM pitlake_dev.silver.edgar_filings)
  UNION ALL SELECT 'edgar_company_facts',
         (SELECT count(*) FROM pitlake_dev.bronze.edgar_company_facts),
         (SELECT count(*) FROM pitlake_dev.silver.edgar_company_facts)
)
SELECT t.silver_table, t.bronze_rows, l.judged, t.silver_rows, l.kept,
       coalesce(q.quarantine_rows, 0) AS quarantine_rows, l.quarantined, l.files,
       t.bronze_rows = t.silver_rows + coalesce(q.quarantine_rows, 0) AS adds_up
FROM tables t
LEFT JOIN logged l USING (silver_table)
LEFT JOIN quarantine q USING (silver_table)
ORDER BY t.silver_table;
```

✅ Seven rows, every `adds_up` = `true`, `bronze_rows` = `judged`, `silver_rows` = `kept`, and
`quarantine_rows` = `quarantined`. Binance shows 49675597 Bronze rows.

Then check the timestamps (both counts must be 0):

```sql
SELECT 'binance' AS t, count_if(knowable_at IS NULL) AS missing, count_if(knowable_at < event_time) AS early
FROM pitlake_dev.silver.binance_spot_trades
UNION ALL SELECT 'facts', count_if(knowable_at IS NULL), count_if(knowable_at < event_time)
FROM pitlake_dev.silver.edgar_company_facts;
```

And see how each fact got its knowable_at. Facts from filings in the filings index use
`acceptance_time`; older ones, beyond the index's reach, use `end_of_filing_date`:

```sql
SELECT symbol, knowable_basis, count(*) AS facts, min(filed_date), max(filed_date)
FROM pitlake_dev.silver.edgar_company_facts GROUP BY ALL ORDER BY ALL;
```

### A5. A re-run transforms nothing twice

```bash
make transform
```

✅ `TERMINATED SUCCESS`, and every table prints `0 files to transform`. Run A4's first query again:
the numbers are identical.

---

## Part B: prod

### B1. Deploy

```bash
make deploy-prod
```

✅ `Deployment complete!`. `pitlake-transform-prod`'s trigger shows *Active*, so the job now starts
by itself after each daily ingest commit. Each of those runs transforms up to 30 files per table.

### B2. First chunk: measure how long a run takes

```bash
databricks bundle run transform -t prod --params max_files=30
```

✅ `TERMINATED SUCCESS`. Note how many minutes it took. The Binance iteration is the slow one,
since 30 days are about 100 million trades.

### B3. Transform the rest

| B2 took | Command to repeat |
|---|---|
| Under 15 minutes | `databricks bundle run transform -t prod --params max_files=120` |
| 15 minutes or more | `databricks bundle run transform -t prod --params max_files=60` |

Repeat until every table in one run prints `0 files to transform`. Binance has about 650 daily
files, so expect several runs over a few days.

❌ If a run stops because the compute quota ran out, continue tomorrow. Nothing is lost: progress
is committed per batch, and the next run carries on.
❌ If the daily triggered run starts while yours is running, it waits or is skipped. Both are fine.

### B4. Check that prod Silver adds up

Run A4's queries with `pitlake_prod` in place of `pitlake_dev`. ✅ Every `adds_up` is `true`.

---

## Part C: the Stage 3 exit test

### C1. Measure the rejection rates

```sql
SELECT silver_table, rule, rows_rejected, rows_judged, round(100 * rejection_rate, 4) AS pct,
       description
FROM pitlake_prod.silver.rejection_rates
ORDER BY silver_table, position;
```

✅ One row per table and drop rule, including the rules that rejected nothing. **Take a
screenshot.** Then pull an example of each kind of rejection:

```sql
SELECT silver_table, rule, count(*) AS rows, min(_period_start) AS first_day,
       max(_period_start) AS last_day, min(record) AS example
FROM pitlake_prod.silver.quarantine
GROUP BY ALL ORDER BY silver_table, rule;
```

And the sequence gaps, which are recorded and not rejected:

```sql
SELECT silver_table, _symbol, count(*) AS gaps, sum(missing) AS missing,
       min(_period_start) AS first_day, max(_period_start) AS last_day
FROM pitlake_prod.silver.sequence_gaps GROUP BY ALL ORDER BY ALL;
```

Send Claude the results of all three queries. Claude explains every category with a nonzero count
and records the explanations in ADR 0006. That is the first half of the exit test.

### C2. Prove that the timestamp rules halt the run

First note what is committed now:

```sql
SELECT * FROM pitlake_prod.control.silver_commits
QUALIFY row_number() OVER (PARTITION BY silver_table ORDER BY committed_at DESC) = 1;
```

Then inject each fault into the first quarter of 2025. Every table has data there.

```bash
databricks bundle run transform -t prod --params reprocess=true,period_start=2025-01-01,period_end=2025-03-31,inject_fault=knowable_at_missing
databricks bundle run transform -t prod --params reprocess=true,period_start=2025-01-01,period_end=2025-03-31,inject_fault=knowable_at_before_event
databricks bundle run transform -t prod --params datasets=edgar.company_facts,reprocess=true,period_start=2025-01-01,period_end=2025-03-31,inject_fault=knowable_at_before_event
```

✅ Each run ends **`TERMINATED FAILED`**. That is the expected result: every `transform` iteration
failed with
`QualityHalt: <table>: halt rules failed (knowable_at_present: <n> rows); nothing was written`
(or `knowable_at_not_before_event` for the second fault), and `commit` shows *Upstream failed*.
The third run covers EDGAR facts, which the second run never reaches because filings halt first in
the same iteration. Each iteration retries once, so a run takes a few minutes, and each halt is
recorded twice.

```sql
SELECT injected_fault, silver_table, rule, count(*) AS halts, max(failed_rows) AS rows_hit
FROM pitlake_prod.control.quality_halts
WHERE injected_fault <> 'none'
GROUP BY ALL ORDER BY ALL;
```

✅ For `knowable_at_missing`, the 6 tables other than EDGAR facts, with rule
`knowable_at_present`. For `knowable_at_before_event`, all 7 tables, with rule
`knowable_at_not_before_event`. Run the
`silver_commits` query from the top of C2 again: ✅ **nothing changed**. **Take screenshots** of
both, and send them to Claude with the three Run URLs. **Stage 3 is complete** when C1's categories
are explained and C2 passes.

---

## Every day

Nothing to do: the ingest job commits at about 06:30 UTC, and the commit starts the transform job.
A failure email means a run failed after its retries. If the failed task's output says
`QualityHalt`, run

```sql
SELECT * FROM pitlake_prod.control.quality_halts ORDER BY halted_at DESC LIMIT 20;
```

and send Claude the result. The data that broke the rule is in Bronze but not in Silver, so
nothing downstream has seen it.

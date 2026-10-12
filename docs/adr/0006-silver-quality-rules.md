# ADR 0006: Silver, quality rules and knowable_at

Status: accepted (Stage 3). Builds on [ADR 0005](0005-parallel-ingest-and-commit.md): Silver reads
only the Bronze versions the ingest job has committed.

## Context

Stage 3 adds the cleaning layer: typed columns, duplicates removed, timestamps normalised, symbols
reconciled, the quality rules of spec section 5 enforced, rejected rows quarantined, and the
knowable-at timestamp of spec section 6 assigned. It is complete when rejection rates are measured
and every category explained, and when the timestamp rules are proven to halt the run on
deliberately corrupted input.

## Decisions

**A Transform job of wheel tasks, not a Declarative Pipeline.** Spec section 9 names Declarative
Pipelines for the cleaning layer. They were set aside for four reasons. Silver must read each Bronze
table `VERSION AS OF` its committed version (ADR 0005), and a pipeline reads live tables or
streams. Reprocessing one Bronze file has to replace exactly its Silver rows, the same idempotent
`replaceWhere` that Bronze uses. A drop expectation discards a row without keeping it, so a
quarantine would mean judging every row twice. And Free Edition's pipeline capacity is limited
(spec section 12). The spec's real requirement still holds: rules are declared in one place, not
scattered through code. Each Silver table is a `SilverSpec` in `pitlake.silver`, its rules are
`Rule(name, condition, action, description)` values in `pitlake.quality`, and every run publishes
them to `control.quality_rules`. The job has no branch per source, as in Stage 2.

**Rows are judged by the first rule they fail, in a fixed order.** Every table starts with the same
five rules, followed by its own:

| # | Rule | On failure |
|---|---|---|
| 1 | `record_parses`: Bronze split the record into columns | drop |
| 2 | `source_registered`: the dataset is in `control.source_registry` | halt |
| 3 | `timestamp_plausible`: the event time is where this source's rows can be | drop |
| 4 | `knowable_at_present` | halt |
| 5 | `knowable_at_not_before_event` | halt |
| … | `key_present`, then the table's checks (`price_and_quantity_positive`, `ohlc_consistent`, `value_reported`, …) | drop |
| last | `duplicate_key`: a row with the same key is already kept | drop |
| — | `sequence_continuous` (Binance trade ids, Coinbase minutes) | record only |

A condition that is NULL counts as failed. The plausibility rule runs before the knowable_at rules
on purpose. Where knowable_at is the event's own timestamp (trades, candles), a missing or misread
time is an implausible timestamp, which is a drop. The knowable_at rules then catch what they exist
for: a knowable_at that comes from somewhere else (a FRED vintage date, an EDGAR acceptance time) and
is missing, or one that was mis-assigned. Duplicates are judged among rows that pass everything else,
so a bad copy is quarantined for what is wrong with it and the good copy is kept. The winner is
deterministic (earliest partition, then file, then raw record), so a re-run keeps the same row.

**knowable_at per source.**

| Table | event_time | knowable_at |
|---|---|---|
| `binance_spot_trades` | trade time (ms before 2025-01-01, µs after) | event_time |
| `coinbase_spot_candles_1m` | minute start | end of the minute |
| `alpaca_stock_trades` | trade time | event_time |
| `alpaca_stock_bars_1d` | bar time | end of the New York day (the bar includes after-hours) |
| `fred_series_vintages` | observation date | end of the vintage date, New York |
| `edgar_filings` | SEC acceptance time | acceptance time |
| `edgar_company_facts` | period end | acceptance time of the filing that reported the fact (from `edgar_filings`), else the end of its filing date, New York |

A date-only time resolves to the next midnight in New York, because the hour is unknown and any
earlier guess could leak a value before it was published. `edgar_company_facts.knowable_basis`
says which rule applied. The filings index only lists a company's recent filings, so older facts
use the later, safe end-of-day time. A restated value has its own accession number, so it is a new
row, never an overwrite.

**A halt writes nothing.** Each batch is judged once and staged as Parquet in the `silver.staging`
volume, and its rows are counted per first failed rule. If any halt rule failed, the task records
the rule, its count and sample rows in `control.quality_halts` and raises `QualityHalt` before
writing Silver, quarantine or the load log. The iteration fails, the failure email goes out, and
the commit does not run.

**One quarantine table.** `silver.quarantine` holds every dropped row: the Silver table, the rule,
and the Bronze record as received (JSON). Every Bronze row ends up either in its Silver table or
there, so `silver.rejection_rates` (rejected rows per drop rule, with the rule's description) adds
up. The table is partitioned by `silver_table` so that iterations running in parallel replace
disjoint partitions. `silver.sequence_gaps` is built the same way.

**Incremental by file, the Bronze pattern again.** `control.silver_load_log` records each Bronze
file and SHA-256 that Silver has judged, with row counts per rule. A run transforms the files at the
committed Bronze version that are not in the log, up to `max_files` per table, and replaces their
rows with `replaceWhere _source_file IN (…)`. EDGAR is the exception: its snapshots repeat most
facts, so its tables are symbol-scoped. A new snapshot re-judges every snapshot of that company
together (`replaceWhere _symbol IN (…)`), and a repeated fact is a `duplicate_key` rejection
attributed to the later snapshot. Change data feed was considered and not needed.

**Silver has its own commit record.** `commit` runs only if every transform iteration succeeded
and appends each Silver table's Delta version to `control.silver_commits`, as the ingest job does
for Bronze. Gold (Stage 5) reads Silver `VERSION AS OF` its committed version
(`pitlake.jobs.commit_transform.latest_silver_commits_query`), so a run that halted part-way never
reaches the research layer.

**The job starts when Bronze is committed, not every hour.** Spec section 7 schedules Transform
hourly, but Silver can only change after an ingest commit, which happens once a day. An hourly
schedule would start serverless tasks 23 times a day to find nothing, out of a daily quota. The
job uses a table-update trigger on `control.ingest_commits` instead (paused in dev, the bundle
variable `transform_trigger_status`). The same trigger serves Stage 4's cross-venue check and
quality gate.

**Faults are injected on purpose.** The job parameter `inject_fault` (`knowable_at_missing`,
`knowable_at_before_event`) corrupts knowable_at in one row per file, among rows that would reach
the knowable_at rules, during a `reprocess` of a chosen period. The run must halt. If it does not,
the task fails anyway, and nothing is written in either case. This is the exit-test drill, and it
runs against real data on Databricks. The Spark tests repeat it locally and also corrupt the input
itself (a FRED vintage without a date halts the run).

**Spark tests run in CI.** As ADR 0001 planned, `tests/spark` applies the rules with a local Spark
session (ANSI mode, UTC) to Bronze-shaped rows, including corrupted ones. pyspark is not a project
dependency. CI adds it for that one step (`uv run --with pyspark`), and locally those tests skip.

## Consequences

- Changing a Silver table's columns needs the table dropped (or altered) and its files reprocessed
  with `reprocess=true` over their period. `CREATE TABLE IF NOT EXISTS` does not migrate a schema.
- Silver reads committed Bronze versions, so ADR 0005's retention caveat applies here too. If no
  ingest run commits for longer than Bronze's 7-day file retention, the version can't be read.
- The first prod run must judge about 2.4 billion Binance trades. It is chunked by `max_files`
  like the Bronze backfill, and an interrupted run resumes from the load log.
- Binance's ms-to-µs switch, which ADR 0002 left to Silver, is applied by partition date, so a
  unit error lands outside its day and is dropped, not silently "corrected".
- A fact whose period ends after its filing date is implausible (`timestamp_plausible`), not a
  halt. The first prod run (2026-10-12) halted on five such NVIDIA facts from 2011 to 2014, among
  them a dividend declared for the next quarter and a subsequent-event share repurchase. Their
  knowable_at was right; their periods had not happened when they were filed.
- EDGAR acceptance times must fall within EDGAR's 06:00 to 22:00 New York hours. A filing outside
  them is dropped, and any fact it reported falls back to the end of its filing date.

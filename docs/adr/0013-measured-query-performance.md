# ADR 0013: Measured query performance

Status: proposed (Stage 8). Builds on [ADR 0005](0005-parallel-ingest-and-commit.md),
[ADR 0006](0006-silver-quality-rules.md) and [ADR 0011](0011-operations-costs-and-table-health.md).

## Context

Spec section 9 uses liquid clustering to organise tables by symbol and time and reduce query cost.
The exit test of the operations stage asks for query performance improvements that are measured
and published. Every Bronze, Silver and Gold table has been clustered since it was created
(Bronze on `_symbol, _period_start`, Silver on its symbol and event time, Gold on symbol and bar
time or entity, feature and `knowable_at`), and predictive optimization runs the clustering. The
open question was whether the queries the jobs actually run benefit from it.

`system.query.history` records, for every statement, its duration and the files, bytes and rows it
read, and the files Delta skipped from their statistics. Jobs on serverless record the Python line
that ran as the statement text, so each statement can be traced to its code. Over 14 days the jobs
ran 8,420 statements for 5.9 hours and read 312 GB. `pitlake-transform-prod` alone ran 4.2 hours
and read 262 GB.

## Findings

A steady-state day (2026-10-22 to 10-25, one new file per symbol) read about 2.49 billion rows in
the transform and 2.47 billion in the operations job, while Silver gained about 1.6 million. Three
causes account for nearly all of it:

1. **The file predicates could not skip files.** Bronze loads and Silver writes replace rows with
   `replaceWhere "_source_file IN (...)"`. Delta keeps 32 characters of string statistics, and every
   source path starts with the same 32 (`binance/spot_trades/BTCUSDT/BTCU...`), so every file's
   minimum and maximum include every path. Each write read all 281 Bronze or 793 Silver files of
   Binance to replace one day. The Bronze row counts after a load did the same.
2. **Finding pending files scanned all of Bronze.** The transform listed the distinct source files
   of the committed Bronze version, then removed those in `control.silver_load_log`. For Binance
   that read 2.43 billion rows every run to find one file.
3. **The freshness check scanned all of Silver.** `max(knowable_at)` over every Silver table read
   2.47 billion rows and 6.3 GB in 29 seconds a day.

Gold was not one of them. Its tables are small (`bars_1m` is 101 MB in 21 files, `features` one
file of 0.5 MB), its incremental reads of Silver already skip 793 of 795 files, and the `gold`
task's time goes to the leakage audit's joins and window functions, not to reading.

## Decisions

**Every replace predicate names the clustering columns.** `bronze.replace_where_predicate` takes
each file's path, symbol and period and returns `_symbol IN (...) AND _period_start IN (...) AND
_source_file IN (...)`. Bronze is clustered on the first two, and Silver's event time follows the
period, so `_period_start`'s statistics skip the other files. The predicate matches the same rows
as before: the manifest requires a symbol and a period for every file, they come from the landing
path, and a file's rows all carry them. A file without them is refused rather than written with a
predicate that would match nothing. The Bronze row counts, the Silver write and its quarantine and
sequence-gap writes all use it.

**Pending files start from the load logs.** Before reading Bronze, the transform anti-joins
`control.bronze_load_log` with `control.silver_load_log` to find the symbols and periods that have
files Silver has not seen. Both logs are small. No candidates means no Bronze read at all. Otherwise
the Bronze query keeps its anti-join and adds `_symbol IN` and `_period_start IN` for the candidates,
so the committed Bronze version still decides which files exist. `load_bronze` writes the log after
each Bronze commit and selects by the same (path, sha256), so a file whose log row was lost to a
crash is loaded again by the next ingest and then found. On 2026-10-26 every file in all eight
prod Bronze tables was in the log with the same sha256, symbol and period. A `--reprocess` run
reads the period it is given, as before. Change data feed would do the same job, but only for
versions after it is turned on, and the logs already exist.

**Freshness reads only rows from the last check on.** The `freshness` step filters each Silver
table on `knowable_at` at or after the newest value the previous check recorded. A maximum at or
above that floor is the table's maximum, and `knowable_at` follows the clustered event time, so
the filter skips almost every file. If a table has no such row, which only a reload that lowered
its newest value can cause, that table is scanned in full.

**Query statistics are recorded and shown.** The operations job's `queries` step writes
`control.query_stats`, with queries, seconds, files read and skipped, bytes and rows per job or
client per day. Like `control.job_costs`, it rewrites the last 30 days on each run. The console's
Performance tab charts them per job. The comparison below can be repeated from that table.

## Measurements

Read-only probes on the Serverless Starter Warehouse, 2026-10-26, on prod:

| Query | Files read | Files skipped | Rows read | Bytes read | Time |
|---|---|---|---|---|---|
| Silver Binance rows of one file, `_source_file IN` | 789 | 0 | 1,588,915 | 207 MB | 5.8 s |
| Same, with `_period_start IN` | 1 | 788 | 1,588,915 | 0.26 MB | 0.7 s |
| Bronze Binance rows of one file, `replace_where_predicate` | 2 | 280 | 1,588,915 | 0.52 MB | 0.9 s |
| Silver Binance rows of one file, `replace_where_predicate` | 1 | 788 | 1,588,915 | 0.26 MB | 0.8 s |
| Pending Binance files, whole Bronze version | 285 | 3 | 2,429,358,379 | 74 MB | 19.3 s |
| Candidates from the two load logs | 5 | 6 | 1,415 | 0.44 MB | 0.8 s |
| Pending Binance files for one candidate day | 5 | 283 | 1,589,577 | 0.75 MB | 1.1 s |

The freshness query with floors returned the same newest `knowable_at` for all eight tables as the
full scan.

Baseline in the jobs, the four scheduled days 2026-10-22 to 10-25 (prod ingest and transform):

| Statement | Runs | Files read | Files skipped | Rows read | Bytes read | Time |
|---|---|---|---|---|---|---|
| Pending files | 32 | 1,519 | 502 | 9,880,411,481 | 327 MB | 133 s |
| Bronze replace | 15 | 1,616 | 0 | 10,350,799 | 1,214 MB | 136 s |
| Bronze row counts | 14 | 1,327 | 0 | 10,626,432 | 310 MB | 18 s |
| Silver replace | 14 | 3,276 | 0 | 10,626,432 | 1,046 MB | 102 s |
| Quarantine and gap replaces | 22 | 122 | 102 | 10,367,951 | 62 MB | 61 s |

Per day, the transform read about 2,500 files and 2.49 billion rows in 317 to 357 seconds of
statements, ingest about 900 files in 116 to 148 seconds, and the freshness check 2.47 billion rows.

## Consequences

- A new dataset needs a symbol and a period per file, which the manifest already requires.
- A predicate on a long string column cannot skip files here. New filters go on the clustering
  columns or on dates and timestamps, whose statistics are exact.
- `control.query_stats` holds every workload in the workspace, as `control.job_costs` does, so the
  dev and prod catalogs record the same rows.
- In a serverless job, `system.query.history` stores the Python line that ran a statement as its
  `statement_text`, not the SQL. A job's statements are found by joining
  `query_source.job_info.job_id` to `system.lakeflow.jobs` and matching the call, such as
  `freshness_query(`.

## Result (prod, 2026-10-26)

The first scheduled runs on the change (commit c995486) were ingest 33675339722764 and transform
384520012883380, each with one new file per symbol. From `control.query_stats` and
`system.query.history`:

| Per day | Before (2026-10-22 to 10-25) | 2026-10-26 |
|---|---|---|
| Transform: rows read | 2.49 to 2.50 billion | 18.7 million (−99.3%) |
| Transform: bytes read | 520 to 614 MB | 223 MB |
| Transform: files read / skipped | 2,428 to 2,545 / 2,062 to 2,621 | 1,299 / 3,258 |
| Transform: statement time | 317 to 357 s | 282 s |
| Ingest: files read / skipped | 691 to 1,194 / 132 to 140 | 107 / 756 |
| Ingest: bytes read | 275 to 478 MB | 112 MB |

| Statement | Before, per day | 2026-10-26 |
|---|---|---|
| Pending files | 8 queries, 2.47 billion rows, 380 files, 33 s | 2 queries, 1.4 million rows, 8 files, 3.3 s; the six tables without a new file read no Bronze |
| Bronze replace | 304 to 607 files read, none skipped, 24 to 38 s | 3 read, 304 skipped, 22 s |
| Bronze row counts | 332 files, none skipped | 3 files, 304 skipped |
| Silver replace | 819 files read, none skipped, 25 s | 5 read, 794 skipped, 8 s |

The jobs' wall-clock time did not change much: transform 8.4 minutes against 8.2 to 8.8, ingest
8.2 against 7.7 to 10.1. What remains is fixed cost, mostly starting a serverless task for every
for_each iteration plus the Gold leakage audit, whose joins and windows read little. The change
removed the reads that grew with history. Before it, a year more of trades meant a year more
read every day; now each day reads that day.

The freshness step, from the statements of `pitlake-operations-prod` in `system.query.history`:

| Per day | Before (2026-10-22 to 10-25) | 2026-10-26 and 10-27 |
|---|---|---|
| Files read / skipped | 836 to 842 / 0 | 30 to 33 / 806 to 807 |
| Rows read | 2.47 billion | 1.4 to 3.4 million (−99.9%) |
| Bytes read | 6.3 to 11.7 GB | 12 to 17 MB |
| Time | 29 to 35 s | 11 to 12 s, plus 11 s to read the floors from `control.freshness` |

It had been nearly all of the operations job's reading: the job read 2.47 billion rows a day
from 2026-10-23 to 10-25 and 1.42 million on 2026-10-26 (`control.query_stats`). Time fell less
than reads, 22 seconds for two statements against about 30 for one, because a statement in a
serverless job takes about 10 seconds however little it reads.

# ADR 0014: Genie spaces over Gold and the control tables

Status: proposed (Stage 8). Builds on [ADR 0008](0008-gold-and-point-in-time-access.md),
[ADR 0011](0011-operations-costs-and-table-health.md) and [ADR 0012](0012-console-app.md), and
amends ADR 0012's rule that the console reads only control tables.

## Context

AI/BI Genie, included in Free Edition, answers questions in plain language by writing SQL against
the Unity Catalog tables of a space, on a SQL warehouse, as the person or service principal that
asks. The CLI deploys a space as a bundle resource (`genie_spaces`) from its serialized definition:
tables, instructions, example questions with their SQL, and benchmark questions with the SQL whose
result is the right answer. Benchmarks can be run against the space (`genie genie-create-eval-run`).

Genie writes its own SQL against the latest version of a table. PITLake's research reads Gold only
through `pitlake.pit`, at the version a transform committed, and never returns a value knowable
after the instant asked for (ADR 0008). A space over Gold that ignored `knowable_at` would answer
"what was Apple's revenue on 2024-08-01" with a figure filed later.

## Decisions

**Two spaces, one per question type.** `pitlake-markets` (`markets_genie`) holds `gold.bars_1d`,
`gold.bars_1m`, `gold.features` and `gold.fundamentals`. `pitlake-operations` (`operations_genie`)
holds 10 control tables: costs, query statistics, freshness, drift, table health, the Silver load
log and rules, the gate results, the cross-venue log and the backtests. A
space answers best when its tables share one subject, and each stays well under Genie's limit of
30 tables. Both are defined in `resources/genie.yml` and run on the warehouse the console uses.

**The markets space answers as of an instant.** Its instructions say that a named date or "as
known" means `knowable_at <=` that instant, then the latest `event_time` and the latest revision
of it, the same order `pit.as_of` uses. A date without a time is 00:00 UTC, as in the `ask` job.
Its example SQL follows that rule, and five benchmarks check it on prod data:

| Benchmark | Right answer | Why it tests point-in-time |
|---|---|---|
| Apple quarterly revenue known at 2024-08-01 22:00 UTC | $90,753M (quarter to 2024-03-30) | The June 10-Q was accepted at 22:03 |
| Apple quarterly revenue known on 2024-08-02 | $85,777M (quarter to 2024-06-29) | Three minutes later it is known |
| US unemployment rate known on 2025-11-15 | 4.3% for August 2025 | September's release was delayed by the shutdown |
| NVIDIA net income first reported for the quarter to 2025-10-26 | $31,910M | First filing, not a restatement |
| Day the Binance and Coinbase bitcoin closes differed most | 2026-02-04 | A join across venues |

Every example and benchmark query was run on prod on 2026-10-26 and returned these values. A test
checks that every features query with a time names `knowable_at <=` and the as-of ordering, that
each query reads only its space's tables, and that the definition's ids are unique and sorted.

**Genie is for exploration, not for research.** It reads the latest Gold tables, not the committed
version, and a question it misreads can still drop the `knowable_at` filter. Backtests and the
retrieval evaluation keep reading through `pitlake.pit` and `gold_commits`. The console says so
next to the question box.

**The console asks Genie as its service principal.** An Ask tab sends the question through the
conversation API, keeps the conversation for follow-ups, and shows Genie's text, the SQL it ran
and the result. The app gets CAN_RUN on both spaces and SELECT on each table they read, declared
in `resources/console.yml`, and a test fails unless the grants equal the console's own tables plus
the spaces' tables. The app can now read Gold, which ADR 0012 ruled out. Gold holds bars, metrics
and features derived from Silver, not raw records, and the grants still name each table.

## Consequences

- A table added to a space needs a grant for the app, and the test says which. An app takes at
  most 20 resources, and the console has 19: the warehouse, the ingest job, the two spaces and 15
  tables. The commit logs were left out of the operations space for that reason; a further table
  needs another one dropped or the spaces asked on behalf of the viewer instead.
- The free tier of the conversation API takes about five questions a minute per workspace.
- Genie's answers are only as good as its instructions; the benchmark accuracy of both spaces is
  recorded below after each change to them.

## Benchmark results

Run 1, dev, 2026-10-26 (markets `01f1d0ea495c11139f40d18a9f2788b7`, operations
`01f1d0ea4bb01460a0d2364de427a0a2`). Genie may add columns to an answer; a missing column or a
different row counts against it.

| Space | Correct | Notes |
|---|---|---|
| Markets | 4 of 5 | All three point-in-time questions filtered on `knowable_at <=` with the as-of order. "The quarter ending 2025-10-26" was read as `period_end <=` that date; dev has no NVIDIA filings, so both results were empty and the judge asked for review |
| Operations | 2 of 4 | Both misses were the benchmarks': the replay count expected two columns where one is a full answer, and "which drop rule" did not say that a rule belongs to one table |

The fixes were an instruction that "the quarter ending <date>" means `period_end =` that date, an
instruction to name and group drop rules by table, a one-column replay benchmark, and a question
that asks for the table and the rule.

Run 2, dev, 2026-10-26 (markets `01f1d0eb4da517579eaedcee9294b6ca`, operations
`01f1d0eb4f3e14e0ba8f5acdaafec3dd`): markets 4 of 5, operations 4 of 4. The point-in-time
questions were right again. Genie still wrote `period_end <=` for "the quarter ending", despite
the instruction, so the space got an example question with `period_end =` instead.

Run 3, dev (markets `01f1d0ec31af1021ab38be6277114a99`): markets 4 of 5, the same filter again.
Genie's reading was the better one. `period_end <=` the date, newest period first, returns the
same row as `=` whenever a quarter ends on that date, and it also finds Apple's June quarter,
which ends on 2024-06-29, when asked for "the quarter ending 2024-06-30", where `=` finds
nothing. The instruction and the example now say "the latest period_end on or before that date",
and the NVIDIA benchmark, empty in dev, became Apple's net income for the quarter ending
2024-06-30 ($21,448M in both catalogs).

Run 4, dev, after commit a957b4a (markets `01f1d0edd5801292bf9860cbd078bbcf`, operations
`01f1d0edffc41c7380db937bfe206397`): **markets 5 of 5, operations 4 of 4**.

Run 5, prod, 2026-10-26 (markets `01f1d11b315d17b7989bb9aef54a3e27`, operations
`01f1d11b645c1324bdfb56dac7f667c6`): **markets 5 of 5** on prod data, NVIDIA included, and
operations 3 of 4. The miss was "Which Silver tables are stale?", whose right answer in prod is
none: Genie listed all eight with `stale` false, which answers the question but is not an empty
result. A benchmark needs an answer that is never empty, so it became "How many Silver tables are
stale?" (0 in prod, 6 in dev).

`make genie-benchmarks TARGET=<dev|prod>` starts a run of both spaces' benchmarks, waits, and
prints each question's assessment, Genie's SQL and the shape of both results;
`make genie-results TARGET=<dev|prod>` prints the latest run without starting one.

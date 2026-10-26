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
holds 13 control tables: costs, query statistics, freshness, drift, table health, the Silver load
log and rules, the gate results, the cross-venue log, the three commit logs and the backtests. A
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

- A table added to a space needs a grant for the app, and the test says which.
- The free tier of the conversation API takes about five questions a minute per workspace.
- Genie's answers are only as good as its instructions; the benchmark accuracy of both spaces is
  recorded below after each change to them.

## Benchmark results

To be added after the first evaluation runs.

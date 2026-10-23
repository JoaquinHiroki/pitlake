# ADR 0012: The console app

Status: proposed (Stage 8). Builds on [ADR 0005](0005-parallel-ingest-and-commit.md),
[ADR 0007](0007-cross-venue-quality-gate.md), [ADR 0009](0009-research-and-reproducible-backtests.md)
and [ADR 0011](0011-operations-costs-and-table-health.md).

## Context

Spec section 8 asks for a Databricks App that shows which data is present for which sources and
dates, current rejection rates by rule, the disagreements found between the two crypto venues, and
a browser for past backtest runs, and that can start the ingestion job for a specific source and
date range. The risks table adds that the application reads only aggregated data, never raw data.
Free Edition allows three apps per account and stops an app 24 hours after it is started, updated
or redeployed.

## Decisions

**The console reads only control tables, through grants the bundle declares.** Every screen comes
from a record the jobs already write, so the app runs no aggregation over Bronze, Silver or Gold:

| Screen | Table | What it shows |
|---|---|---|
| Coverage | `control.silver_load_log`, `control.freshness` | Periods, rows and quarantined rows per Silver table and symbol, rows per period, and the latest freshness check |
| Rejections | `control.silver_load_log`, `control.quality_rules` | Rows each drop rule rejected over the chosen days, divided by the rows judged |
| Crypto venues | `control.cross_venue_log`, `control.quality_gate_results` | Divergent and missing minutes per day, and the latest gate run without an injected fault |
| Backtests | `control.backtest_runs` | Every backtest and replay with its headline metrics, and one run's parameters, table versions, code commit, hashes and MLflow run |
| Ingest | `control.source_registry` | The datasets that can be chosen |

Each source file counts once, at its latest processing. The app `pitlake-console-<target>`
(`resources/console.yml`) runs as its own service principal. Its resources grant SELECT on exactly
these seven tables, CAN_USE on the Serverless Starter Warehouse and CAN_MANAGE_RUN on the ingest
job, and a test checks that the grants and the queries name the same tables. A query of any other
table fails for lack of a grant, not by convention.

**The app starts the ingest job; it does not ingest.** The form sets the job's parameters, and the
job keeps its retries, its all-or-nothing commit and, in prod, the transform it triggers. The job
gained a `datasets` parameter (comma-separated keys, empty for all), which the `plan` task uses to
choose the for_each inputs, and `period_start`/`period_end` now also reach the collector as
`--start`/`--end`. The commit still records every dataset at its current Bronze version, so a run
for one source leaves the others' commit unchanged in content. The form checks what `load_bronze`
would reject (a reload needs both dates), and the job allows one run at a time, so a second
request while a run is active returns the platform's error.

**Streamlit, queried through the SQL connector.** It is preinstalled in the Apps runtime, and the
results are cached for five minutes, so moving between tabs does not query the warehouse again.
The queries and the form's checks live in `app/console.py`, which has no dependencies and is
tested with the rest of the code. The app has a dev and a prod instance, one per catalog, which
use two of the three apps the account allows.

## Consequences

- The app stops 24 hours after each start. It is started when it is needed
  (`databricks bundle run console -t <target>`), and its compute shows in `control.job_costs`
  like any other workload.
- A new screen needs a new grant in `resources/console.yml`, and the test fails until it has one.
- The gate list shows only checks that failed or were waived. On a normal day it is empty, and the
  caption gives the counts.

# ADR 0011: Operations job, cost per job, table health and freshness

Status: proposed (Stage 8). Builds on [ADR 0009](0009-research-and-reproducible-backtests.md) and
[ADR 0010](0010-point-in-time-retrieval-and-fine-tuning.md).

## Context

Spec section 7 defines an Operations job, run daily, that compacts and optimises storage, applies
retention, and reports compute cost per job from the platform's own usage tables. Section 4 keeps
cost records in the control area. ADR 0010 moved this work from Stage 7 to Stage 8.

Two things were already true when Stage 8 started (2026-10-22):

- Predictive optimization manages every table. It had already run clustering, VACUUM and ANALYZE
  in both catalogs (`system.storage.predictive_optimization_operations_history`), and its
  OPTIMIZE versions are why `snapshots.readable_version` exists.
- Gold tables keep 365 days of files and log. The transform job sets that on every run. Replays of
  backtests (ADR 0009) and retrievals (ADR 0010) read old Gold versions, so VACUUM must never cut
  below it. Bronze and Silver keep the 7-day default, as ADRs 0005 and 0006 record.

## Decisions

**The job records maintenance; it does not repeat it.** Running OPTIMIZE and VACUUM ourselves would
duplicate predictive optimization, and the duplicate work would also be billed. Every day, the
`storage` task writes one row per table to `control.table_health`. The row records the table's
files, bytes, clustering columns, both retention properties, when predictive optimization last ran
each operation on it, and how many of those operations failed. Tables that AI Search creates for
its index (`__…`, `event_log_…`) are skipped. The task fails, and the job emails its owner, if a
Gold table's retention is no longer 365 days. That is the retention the job "applies": it ensures
that no VACUUM can break a replay.

**Transient staging is cleared after a week.** The staging volumes of Bronze, Silver and Gold hold
per-run copies, each comment saying it is safe to empty. A run folder whose newest file is more
than 7 days old is deleted. A volume's directories carry no reliable modification time, so the
age comes from the files. The generator's folders sit one level deeper
(`gold/staging/generator/<run>`), and their adapters and answers are also in MLflow.

**Cost comes from `system.billing.usage` at list price.** The `costs` task joins usage with
`system.billing.list_prices`, using the price in force when the usage ended, and with
`system.lakeflow.jobs` for the job's latest name. It writes daily quantity and dollars per workload
to `control.job_costs`. A workload is a job's name, or the product for usage that belongs to no job
(vector search, model serving, storage, SQL, predictive optimization). Free Edition is not billed,
but list price is what makes different units comparable (DBU, DSU, GB), and the daily compute
quota is spent the same way. Usage arrives late, so each run rewrites the last 30 days in one
atomic `replaceWhere` write. `unpriced` counts usage rows without a list price, so a missing price
shows up instead of reading as zero.

The table covers the whole workspace, dev and prod together, because usage is recorded per
workspace. Each job's name ends with its target.

**Staleness is the age of the newest knowable row, against a tolerance per dataset.** Spec
section 8 asks for staleness on Silver to be detected on the day it happens. Every Silver table has
`knowable_at`, so the same measure works for every source: how many days before the check the
newest row became knowable. The last Silver commit is recorded next to it, which tells an upstream
gap (nothing new published or collected) apart from a pipeline gap (nothing committed). Sources
publish at very different cadences, so each `Dataset` carries `stale_after_days`. The values come
from prod's largest gap between consecutive knowable days since 2025-01-01, measured on
2026-10-22, plus the day ingest takes:

| Silver table | Largest gap | Newest on 2026-10-22 | Stale after |
|---|---|---|---|
| binance_spot_trades, coinbase_spot_candles_1m | 1 day | 1.3 days | 3 days |
| alpaca_stock_trades, alpaca_stock_bars_1d | 4 days (holidays) | 1.1 to 1.4 days | 6 days |
| fred_series_vintages | 23 days | 7.1 days | 30 days |
| edgar_filings | 12 days | 19.4 days | 45 days |
| edgar_company_facts, edgar_filing_documents | 70 days | 56.4 days | 100 days |

The `freshness` task writes one row per table to `control.freshness` every day. In prod it fails,
and the job emails its owner, when any table is past its tolerance or empty. Dev holds a fixed
historical window, so there it only records (`freshness_enforced`). The job runs at 10:00 UTC,
after the 06:30 ingest and the transform it triggers, so the check sees the day's data.

**Lakehouse Monitoring watches Silver's distributions, windowed by `knowable_at`.** Section 8 also
asks for changes in data distribution to be found on the day they happen, so that a change in an
upstream format shows up at once. The `monitors` task gives every Silver table with daily or weekly
data a time-series monitor on `knowable_at`. Each window then holds the rows that became knowable
in it, and a format change lands in the window of the day it arrived. Profile and drift metrics
go to `<catalog>.monitoring.<table>_profile_metrics` and `_drift_metrics`, and drift compares each
window with the one before it.

| Silver table | Window | Sliced by | Rows in the last 30 days (prod, 2026-10-22) |
|---|---|---|---|
| binance_spot_trades | 1 day | | 79,258,561 |
| coinbase_spot_candles_1m | 1 day | | 42,716 |
| alpaca_stock_trades | 1 day | symbol (4) | 1,971,632 |
| alpaca_stock_bars_1d | 1 week | | 88 |
| fred_series_vintages | 1 week | series_id (3) | 2,767 |

The EDGAR tables are not monitored. They had no new rows in the last 30 days, or 14 filings in
the case of `edgar_filings`, and they arrive in quarterly bursts, so a window has too few rows to
have a distribution. Their freshness check and quality rules cover them.

The settings are part of each `SilverSpec` (`monitor_granularity`, `monitor_slices`), like the
rules. The task creates a missing monitor, updates one whose settings changed, refreshes every
monitor and waits for the refreshes. A failed refresh fails the job. The monitors are not bundle
resources, because the transform creates the Silver tables: on a fresh workspace a bundle monitor
would be deployed before its table exists. Monitors have no schedule of their own, so the refresh
always follows the day's transform. A time-series monitor computes only the last 30 days, so a
refresh of `binance_spot_trades` reads about 80 million of its 2.4 billion rows. Change data feed,
which would make refreshes incremental, stays off until the refresh cost in `control.job_costs`
shows that it is needed. Each monitor's dashboard is under the bundle's `monitoring` folder, on
the warehouse that `warehouse_id` finds by name.

## Consequences

- The first look at the numbers (2026-10-21, list price) shows that the AI Search endpoint costs
  about $6 a day: 85 DBUs on the serverless real-time inference SKU, which is more than all the
  jobs together (about $5). An endpoint stays up whether or not it is queried. The `ask` job
  is the only caller outside the rag job, so deleting the endpoint between uses, or replacing the
  index with exact search for a corpus this small, is the largest saving available.
- `control.table_health` is the history that Lakehouse Monitoring and the app will read for
  storage trends.
- A table that predictive optimization skips shows as a row with an empty `last_maintained`.
- The monitors measure drift but do not decide when it matters. On millions of rows every
  statistical test rejects, and prices move by design, so the alert thresholds will be calibrated
  on the first 30 days of drift metrics, as the freshness tolerances were on prod's history.
- Dev's market data is from January 2025, outside the 30 days a monitor computes, so dev's
  monitors prove the task and the deploy but have almost no windows.

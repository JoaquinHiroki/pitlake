# ADR 0011: Operations job, cost per job and table health

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

## Consequences

- The first look at the numbers (2026-10-21, list price) shows that the AI Search endpoint costs
  about $6 a day: 85 DBUs on the serverless real-time inference SKU, which is more than all the
  jobs together (about $5). An endpoint stays up whether or not it is queried. The `ask` job
  is the only caller outside the rag job, so deleting the endpoint between uses, or replacing the
  index with exact search for a corpus this small, is the largest saving available.
- `control.table_health` is the history that Lakehouse Monitoring and the app will read for
  storage trends.
- A table that predictive optimization skips shows as a row with an empty `last_maintained`.

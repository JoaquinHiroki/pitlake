# ADR 0005: Parallel ingest and the commit record

Status: accepted (Stage 2). Settles what [ADR 0004](0004-api-sources.md) left open.

## Context

Spec section 7 asks the daily ingest job to "determine which data is missing, load new files from
each source in parallel, then advance the record of what has been processed only if every source
succeeded". Until now, `collect` handled every feed one after another in one task, `load_bronze`
did the same for every dataset, and the only record of progress was `control.bronze_load_log`,
which advances batch by batch. That per-batch log is what lets an interrupted backfill resume
(Stage 1, and the Alpaca backfill of 2026-10-05), so it cannot simply become all-or-nothing.

## Decisions

**Two records, with different jobs.** `control.bronze_load_log` stays as it is: internal progress,
advanced per batch, used only to decide which files are pending. The record the specification means
is a new table, `control.ingest_commits`: one row per registered dataset per run in which every
source succeeded, holding the Bronze table's Delta version at that moment. Layers downstream of
Bronze read each table `VERSION AS OF` its latest committed version
(`pitlake.jobs.commit_ingest.latest_commits_query`), never the live table. Files a partly failed
run did load are therefore in Bronze but invisible downstream until a run in which every source
succeeds commits them. A reload that rewrites a period in place is invisible too, until committed.

**The job is planned from the registry.** A first task, `plan`, writes the dataset registry in
`pitlake.datasets` to `control.source_registry` and passes the dataset keys to the job as the task
value `datasets`. The code stays the single source of truth; the table is how SQL and later stages
check that a source is registered (spec section 5, "Source is registered: halt run"). `plan` also
creates every control and Bronze table, so parallel iterations never race to create one.

**Collect and load are for_each tasks over the registry.** Each iteration handles one dataset:
`pitlake-collector sync --feed <source.dataset>` and `load_bronze --dataset <source.dataset>`.
A registered dataset that the target's collector config does not list is a quiet no-op, so dev can
carry a subset. Iterations run `ingest_concurrency` at a time (default 3), a bundle variable,
because each one is its own serverless task.

**Discovery stays one task.** Auto Loader keeps one checkpoint for the whole landing volume; one
discovery per source would mean one stream and checkpoint per source directory for no gain, since
discovery takes about a minute.

**Commit runs only if everything succeeded.** `commit` depends on collect, discover and load with
`run_if: ALL_SUCCESS`. It writes all datasets' rows in one append, so a commit covers every dataset
or none. A source that fails still has its files landed and loaded where possible (discover keeps
`run_if: ALL_DONE`), the failure email goes out, and the next fully successful run commits
everything at once.

## Consequences

- Wall-clock time drops to roughly that of the slowest source; the 2026-10-05 Alpaca backfill ran
  its eight feeds one after another for 1 h 40 min.
- Iterations can share a serverless machine while running as different users, so a fixed scratch
  path such as `/tmp/pitlake-collector` belongs to whichever iteration created it and the others
  get `Permission denied` (first dev run, 2026-10-05). The collector downloads into a fresh private
  directory instead (`--private-work-dir`).
- Each iteration pays serverless start-up, so a quiet daily run costs more task-minutes than one
  sequential task did.
- Delta time travel is now part of the contract. If no run commits for longer than Bronze's
  retention (`delta.deletedFileRetentionDuration`, 7 days by default, enforced by VACUUM), the
  committed version can no longer be read and downstream jobs fail rather than read a newer one.
- The Stage 2 exit test holds: adding a source is a collector class, a `Dataset` entry and a config
  feed; the job definition does not change.

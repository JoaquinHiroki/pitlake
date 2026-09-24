# ADR 0001: Repository and deployment architecture

Status: accepted (Stage 0)

## Decisions

**Monorepo, two deployables.** `src/pitlake` is the platform package: it is built into a wheel and runs
inside Databricks jobs. The collector (added in Stage 1, under `collector/`) is a separate package with
its own dependencies, deployed to the VM. They share naming rules through `pitlake.config`, and the
collector depends only on the Databricks SDK, never on Spark.

**Everything in Databricks is a bundle resource.** Schemas, volumes and jobs are declared in
`databricks.yml` and `resources/`. The only things created outside the bundle are the two catalogs
(`scripts/bootstrap.sh`), because a bundle cannot deploy schemas into a catalog that does not exist yet.

**Dev and prod are separate catalogs in one workspace.** Free Edition provides a single workspace. The
`dev` and `prod` bundle targets differ only in the `catalog` variable (`pitlake_dev`, `pitlake_prod`).
Dev holds a small sample (a few symbols, a few weeks) so it does not burn the daily compute quota that
prod needs.

**Jobs run a versioned Python wheel on serverless compute.** Business logic lives in the package and is
unit-tested locally and in CI; job definitions stay thin. Notebooks are not used for pipeline logic.

**Tests that need Spark run locally with pyspark, not on Databricks.** From Stage 3 onward, quality-rule
and point-in-time tests run in GitHub Actions against a local Spark session, so the leakage suite
costs no Databricks quota and runs on every push.

**Authentication.** Local: OAuth via `databricks auth login` into a profile named `pitlake`. CI and the
VM: a token stored as a GitHub secret or in a root-only file on the VM. No credential or workspace host
is committed.

## Consequences and hazards

- `databricks bundle destroy` deletes schemas and volumes, including raw files. It is only wired to
  `dev` in the Makefile. Never run it against `prod`.
- Removing a schema from `resources/storage.yml` and redeploying to prod would try to drop it. Review
  `databricks bundle deploy` plan output before confirming any deletion.
- Bundle-managed catalogs are avoided on purpose: catalog creation on Free Edition depends on the
  workspace's default storage and is a one-time action.

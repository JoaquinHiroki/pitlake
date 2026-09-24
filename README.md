# PITLake

A point-in-time market data lakehouse built on Databricks. Every row carries a `knowable_at`
timestamp, and features can only be read through a function that filters on it, so backtests cannot
see information that did not yet exist. Full specification: [docs/PITLake-project-specification.pdf](docs/PITLake-project-specification.pdf).

## Status

Stage 0 - repository, deployment configuration, catalogs, CI.

## Prerequisites

- macOS or Linux with `git`, `make`
- [uv](https://docs.astral.sh/uv/) and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install)
- A Databricks Free Edition workspace

## Deploy from scratch

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile pitlake
make setup        # Python environment
make check        # lint + tests
make bootstrap    # one-time: create the pitlake_dev and pitlake_prod catalogs
make deploy       # schemas, volume and jobs into the dev catalog
make smoke        # runs the deployment smoke test on serverless compute
```

## Layout

| Path | Purpose |
|---|---|
| `databricks.yml`, `resources/` | The platform as configuration (Asset Bundle) |
| `src/pitlake/` | Package built into a wheel and run by jobs |
| `tests/` | Unit tests, run on every push |
| `scripts/` | One-time setup helpers |
| `docs/adr/` | Architecture decisions |

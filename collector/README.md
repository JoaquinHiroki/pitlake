# PITLake collector

Fetches files from public sources, verifies them against the source's checksum, and lands them
untouched in the `raw.landing` volume, followed by a manifest that marks each file complete.
Design: [ADR 0002](../docs/adr/0002-landing-and-bronze.md) (landing contract) and
[ADR 0003](../docs/adr/0003-collector-on-databricks.md) (where it runs).

## Where it runs

Normally as the `collect` task of the `pitlake-ingest-<target>` job, reading
[config.dev.toml](config.dev.toml) or [config.prod.toml](config.prod.toml) and writing through the
`/Volumes` mount. Deploying the bundle deploys the collector; there is nothing else to install.

If Databricks ever blocks outbound access, the same package runs on a Linux VM under
[the systemd unit](deploy/pitlake-collector.service), uploading through the Files API. The procedure is
the fallback section of the [runbook](../docs/runbooks/stage1-setup.md).

## Running it by hand

| Purpose | Command (from the repo root) |
|---|---|
| Try it with no Databricks at all | `make collector-try` (two days into `./data/landing`) |
| Land the dev sample from a laptop | `make collector-dev` (Files API, `pitlake` CLI profile) |

`sync` runs one cycle and exits; it fails if any file failed. Log events (JSON lines): `landed`,
`feed synced` (one per symbol, with counts), `not yet published`, `missing upstream` (a gap older than
two days at the source), `fetch failed` and `upload failed` (retried on the next run).

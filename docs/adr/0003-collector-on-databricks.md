# ADR 0003: The collector runs inside Databricks

Status: accepted (Stage 1). Amends ADR 0001 and section 3 of the specification (revision 3).

## Context

The specification put acquisition on an external VM because Free Edition serverless compute was
understood to have no outbound access to market data APIs. On 2026-09-28 a serverless job in this
workspace reached every Stage 1 and Stage 2 source:

| Source | Endpoint tested | Result |
|---|---|---|
| Binance | data.binance.vision | HTTP 200 |
| Coinbase | api.exchange.coinbase.com | HTTP 200 |
| Alpaca | data.alpaca.markets | HTTP 401 (reachable; needs a key) |
| SEC EDGAR | www.sec.gov | HTTP 200 |
| FRED | api.stlouisfed.org | HTTP 400 (reachable; needs a key) |

## Decision

**The collector is the first task of the ingest job**: `collect`, then `discover_manifests`, then
`load_bronze`. It runs as a Python wheel on serverless compute, reads its configuration from
`collector/config.<target>.toml` in the bundle's workspace files, and writes through the `/Volumes`
mount (`--landing volume`).

**The collector stays a separate package with no Spark dependency.** Nothing in its code knows where it
runs. The Files API landing and the systemd unit remain, so moving it back to a VM is a deployment
change, not a code change. [The runbook](../runbooks/stage1-setup.md) keeps the VM procedure as a
fallback.

**One knob for chunking.** The job's `max_files` parameter limits both how many files `collect` fetches
per symbol and how many `load_bronze` loads per run, so a backfill advances in quota-sized steps.

## Consequences

- One platform to deploy, monitor and pay for. No VM, SSH keys, token rotation or idle reclamation.
- Collection runs on the job schedule (daily) instead of hourly. Binance publishes one file per day,
  so nothing is lost.
- Collection uses serverless compute quota. Measured: about 4 seconds per BTCUSDT day, so a full
  year costs roughly 25 minutes of collect time.
- **Hazard: this depends on Databricks keeping outbound access open.** If Free Edition restricts it,
  `collect` starts failing with connection errors, `discover_manifests` and `load_bronze` still run
  (`run_if: ALL_DONE`), and the failure email arrives. The response is the VM fallback in the runbook.
- The specification's collector properties still hold, except "runs under systemd": verified checksums,
  atomic visibility through the manifest commit marker, rate limiting with backoff, the manifest
  record, and raw files never modified.
- Stage 2 API keys (Alpaca, FRED) go in a Databricks secret scope, not in configuration files.

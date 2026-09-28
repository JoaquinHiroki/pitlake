# PITLake collector

Runs on a Linux VM outside Databricks. Fetches files from public sources, verifies them against the
source's checksum, and uploads them untouched into the `raw.landing` volume through the Databricks
Files API, followed by a manifest that marks the upload complete. Design: [ADR 0002](../docs/adr/0002-landing-and-bronze.md).

## Try it locally (no Databricks needed)

```bash
make collector-try      # fetches two days of BTCUSDT into ./data/landing
```

## Install on the VM

Follow Part B of the [Stage 1 setup runbook](../docs/runbooks/stage1-setup.md). The VM uses
[config.prod.toml](config.prod.toml) and [the systemd unit](deploy/pitlake-collector.service).

## Operating it

| Task | Command |
|---|---|
| Follow logs (JSON lines) | `journalctl -u pitlake-collector -f -o cat` |
| One-off backfill, no file limit | `sudo systemd-run --uid=pitlake -p EnvironmentFile=/etc/pitlake/collector.env -p StateDirectory=pitlake-collector --pty /opt/pitlake/.venv/bin/pitlake-collector sync --config /etc/pitlake/collector.toml --max-files 0` |
| Upgrade | `cd /opt/pitlake && sudo -u pitlake git pull && sudo -u pitlake uv sync --package pitlake-collector --no-dev --frozen && sudo systemctl restart pitlake-collector` |

`sync` exits non-zero if any file failed, so it can also run from cron. Log events to watch:
`landed`, `feed synced` (one per symbol per cycle, with counts), `missing upstream` (a gap older
than two days at the source), `fetch failed` and `upload failed` (retried next cycle).

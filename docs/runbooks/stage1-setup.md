# Stage 1 setup runbook

Everything runs in Databricks: the `pitlake-ingest-<target>` job collects files from Binance into the
landing volume, records their manifests and loads them into Bronze
([ADR 0003](../adr/0003-collector-on-databricks.md)). You need a laptop, the Databricks CLI and uv.

Follow the parts in order. Each step says what you should see. If you see something else, stop and
keep the full output; don't continue to the next step.

- **Part A** (laptop and dev, about 30 minutes): set up, deploy, prove the pipeline end to end on two weeks of data.
- **Part B** (prod, spread over one or two days because of compute quota): load one year and pass the Stage 1 exit test.
- **Part C**: switch on the daily schedule.
- **Appendix** (only if Databricks blocks outbound access): run the collector on an Oracle Cloud VM instead.

---

## Part A: laptop and dev

### A1. Keep the repository out of iCloud-synced folders

If the repository is on the Desktop or in Documents with iCloud sync on, iCloud hides files inside
`.venv` and Python stops finding the project's packages. Keep it in `~/code/PITLake`:

```bash
cd ~/code/PITLake
rm -rf .venv
make setup
ls -lO .venv/lib/python3.12/site-packages/*.pth
```

✅ Every line shows `-` in the flags column, not `hidden`. Run every later command from `~/code/PITLake`.

### A2. Log in to Databricks (once per laptop)

```bash
databricks auth login --host https://dbc-cc51bd40-0056.cloud.databricks.com --profile pitlake
```

✅ A browser tab opens and the terminal prints `Profile pitlake was successfully saved`.
If a later command fails with an authentication error, repeat this step.

### A3. Lint and tests

```bash
make check
```

✅ The last line is `79 passed`.

### A4. Deploy to dev

```bash
make validate
make deploy
make smoke
```

✅ `Validation OK!`, then `Deployment complete!`, then the smoke run ends `TERMINATED SUCCESS`.

### A5. Run the ingest job in dev

```bash
make ingest
```

The command prints a **Run URL** and waits (about 3 to 5 minutes). ✅ It ends with `TERMINATED SUCCESS`
and prints the output of the three tasks:

- `collect`: JSON lines ending with `"event": "feed synced"` and `"failed": 0`. On a fresh dev catalog
  it lands 14 files (2025-01-01 to 2025-01-14); files already landed are skipped.
- `discover_manifests`: `OK: discovered <n> new manifests in pitlake_dev`
- `load_bronze`: `OK: binance.spot_trades loaded <n> files, <rows> rows`

### A6. Check the data in dev

In Databricks, open **SQL Editor**, choose the serverless starter warehouse, and run:

```sql
SELECT count(*)                     AS row_count,
       count(DISTINCT trade_id)     AS unique_trades,
       count(DISTINCT _source_file) AS files,
       min(_period_start)           AS first_day,
       max(_period_start)           AS last_day,
       count(_corrupt_record)       AS corrupt_rows
FROM pitlake_dev.bronze.binance_spot_trades;
```

✅ `row_count` = `unique_trades` = **49675597**, `files` = 14, `first_day` = 2025-01-01,
`last_day` = 2025-01-14, `corrupt_rows` = 0.

### A7. Rehearse the exit test in dev: a reload adds no rows

```bash
databricks bundle run ingest -t dev --params reload=true,period_start=2025-01-01,period_end=2025-01-14
```

✅ `load_bronze` says `loaded 14 files, 49675597 rows`. Run the A6 query again: every number is identical.

### A8. Deploy to prod

```bash
make deploy-prod
databricks bundle run smoke -t prod
```

✅ `Deployment complete!`, then the smoke run ends `TERMINATED SUCCESS`. In **Jobs & Pipelines**,
`pitlake-ingest-prod` shows three tasks: `collect → discover_manifests → load_bronze`. (Since
Stage 2 it shows `plan → collect → discover_manifests → load_bronze → commit`, with collect and load
running one iteration per dataset; see [ADR 0005](../adr/0005-parallel-ingest-and-commit.md).)

---

## Part B: load the year into prod and pass the exit test

Prod collects BTCUSDT for every day of 2025 ([config.prod.toml](../../collector/config.prod.toml)).
The job's `max_files` parameter limits both how many files one run collects and how many it loads,
so the year arrives in chunks that fit the daily compute quota.

### B1. First chunk: measure how long a run takes

```bash
databricks bundle run ingest -t prod --params max_files=30
```

✅ `TERMINATED SUCCESS`, with:
- `collect`: 30 `landed` lines, then `feed synced` with `"landed": 30, "failed": 0`
- `load_bronze`: `OK: binance.spot_trades loaded 30 files, <rows> rows`

Note how many minutes the run took. The runs so far suggest about 5 to 10 minutes.

### B2. Load the rest

Pick the chunk size from B1's duration:

| B1 took | Command to repeat | Runs needed |
|---|---|---|
| Under 10 minutes | `databricks bundle run ingest -t prod --params max_files=120` | about 3 |
| 10 minutes or more | `databricks bundle run ingest -t prod --params max_files=60` | about 6 |

Repeat the command until **both** of these appear in one run's output:
- `collect`: `feed synced` with `"missing": 0`
- `load_bronze`: `binance.spot_trades: 0 files to load`

❌ If a run fails with a message about the **compute quota** being exceeded, stop for the day and
continue tomorrow. Nothing is lost: collected files stay in the volume and progress is committed
every 5 files, so the next run carries on where this one stopped.
❌ If `collect` shows `"failed"` greater than 0, run the same command again; failed files are retried.
If they keep failing, send Claude the `fetch failed` lines.

### B3. Verify the year

```sql
SELECT count(*)                     AS row_count,
       count(DISTINCT trade_id)     AS unique_trades,
       count(DISTINCT _source_file) AS files,
       min(_period_start)           AS first_day,
       max(_period_start)           AS last_day,
       count(_corrupt_record)       AS corrupt_rows
FROM pitlake_prod.bronze.binance_spot_trades;
```

✅ `files` = 365, `first_day` = 2025-01-01, `last_day` = 2025-12-31, `row_count` = `unique_trades`,
`corrupt_rows` = 0. **Take a screenshot.** It is the first half of the exit-test evidence.

### B4. The exit test: reload a month, prove no duplicates

```bash
databricks bundle run ingest -t prod --params reload=true,period_start=2025-03-01,period_end=2025-03-31,max_files=0
```

`max_files=0` matters: the job's default of 30 would leave out March 31.

✅ `load_bronze` says `loaded 31 files`. Run the B3 query again: **all six numbers are identical**.
Take a second screenshot.

```sql
SELECT count(*) AS march_files_reloaded
FROM (SELECT landing_path FROM pitlake_prod.control.bronze_load_log
      WHERE period_start BETWEEN '2025-03-01' AND '2025-03-31'
      GROUP BY landing_path HAVING count(*) >= 2);
```

✅ `31`. A file counts once it has been loaded at least twice, so earlier rehearsals don't matter. **Stage 1 is complete.** Send both screenshots and the Run URLs of B3 and B4 to Claude to record it.

---

## Part C: run every day

Prod has no `end_date` in [collector/config.prod.toml](../../collector/config.prod.toml), and the
prod target sets `ingest_schedule_status: UNPAUSED` in [databricks.yml](../../databricks.yml), so
the ingest job runs daily at 06:30 UTC and collects up to yesterday, oldest missing day first.
Binance publishes each day's file the following day.

```bash
make deploy-prod
```

✅ In **Jobs & Pipelines → pitlake-ingest-prod**, the schedule shows *Active*, daily at 06:30 UTC.

Each scheduled run uses the default `max_files=30`, so a backlog (such as the days since
2025-12-31 when the schedule was first switched on) clears by itself at 30 days per run.
To clear it faster, run B2's command by hand; the scheduled and manual runs never overlap
(`max_concurrent_runs: 1`).

Once days after 2025-12-31 load, B3's query no longer returns 365 files; that number was
the Stage 1 exit test, not an invariant.

❌ A failure email means a run failed after its retries. Open the run, read the failed task's
output, and send Claude the error lines. The next day's run retries whatever is still missing.

---

## Appendix: fallback collector on an Oracle Cloud VM

Use this only if the `collect` task starts failing with connection errors (timeouts, `Connection
refused`, DNS failures) while Binance is up. That means Databricks has closed outbound access; see the
hazard in ADR 0003. The VM uploads through the Files API into the same landing volume, and the rest
of the ingest job works unchanged. Before starting, ask Claude to remove the `collect` task from
[resources/jobs.yml](../../resources/jobs.yml) so the job stops reporting failures.

### V1. Create the Oracle Cloud account

1. Go to https://signup.cloud.oracle.com and choose **Start for free**.
2. Fill in the form. **Home Region cannot be changed later.** Pick a region near you that is **not
   in the United States** (Binance restricts US traffic). Step V5 confirms the choice works.
3. Oracle verifies a credit card with a small temporary hold. Always Free resources are not charged.
4. Wait for the "account is ready" email (a few minutes, occasionally longer), then sign in at
   https://cloud.oracle.com.

### V2. Create an SSH key on your laptop

```bash
ssh-keygen -t ed25519 -f ~/.ssh/pitlake_oci -C pitlake-vm
```

Press Enter at the passphrase prompts, or set a passphrase you will remember. Then copy the public key:

```bash
pbcopy < ~/.ssh/pitlake_oci.pub
```

### V3. Create the instance

In the Oracle console: **☰ menu → Compute → Instances → Create instance**. Labels may differ
slightly from these, but the choices are the same.

| Setting | Value |
|---|---|
| Name | `pitlake-collector` |
| Image | **Change image → Ubuntu → Canonical Ubuntu 24.04** (the standard image, not "Minimal") |
| Shape | **Change shape → Ampere → VM.Standard.A1.Flex**, 1 OCPU, 6 GB memory. It must show the *Always Free-eligible* label |
| Networking | Create new virtual cloud network and a new **public** subnet, and turn on **Automatically assign public IPv4 address** |
| SSH keys | **Paste public keys**, then paste (⌘V) the key you copied in V2 |
| Boot volume | Leave the defaults |

Click **Create**.

❌ If you get **"Out of capacity for shape VM.Standard.A1.Flex"**, change the shape to **Specialty and
previous generation → VM.Standard.E2.1.Micro** (also Always Free) and click Create again.

✅ After 1 to 2 minutes the state is **Running**. Copy the **Public IP address** from the instance page.

### V4. Connect

```bash
ssh -i ~/.ssh/pitlake_oci ubuntu@<PUBLIC_IP>
```

Type `yes` to the fingerprint question. ✅ The prompt becomes `ubuntu@pitlake-collector:~$`.
**Every command in V5 to V12 runs on the VM.**

### V5. Check that Binance is reachable from this region

```bash
curl -s https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip.CHECKSUM
```

✅ `088c0401c491b235bf33ef5884633b2f080c9ba6afb8231f11fd71cea6e2d93c  BTCUSDT-trades-2025-01-01.zip`
❌ If you get an error page or a 403/451 code, the region is blocked. Terminate the instance and use
another region. Ask Claude before doing this.

### V6. Update the system

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y git curl
python3 --version
```

If a purple "Daemons using outdated libraries" screen appears, press Enter.
✅ `Python 3.12.x`. If the upgrade installed a new kernel, run `sudo reboot`, wait 1 minute and reconnect as in V4.

### V7. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
uv --version
```

✅ Prints `uv 0.x.y`.

### V8. Create the service user and install the collector

```bash
sudo useradd --system --create-home --home-dir /var/lib/pitlake --shell /usr/sbin/nologin pitlake
sudo install -d -o pitlake -g pitlake /opt/pitlake
sudo -u pitlake -H git clone https://github.com/JoaquinHiroki/pitlake.git /opt/pitlake
cd /opt/pitlake
sudo -u pitlake -H uv sync --package pitlake-collector --no-dev --frozen
/opt/pitlake/.venv/bin/pitlake-collector --version
```

✅ The last command prints `0.1.0`.
❌ If `git clone` finds no `collector/` directory, the Stage 1 code has not been pushed to GitHub.

### V9. Create a Databricks token for the VM (in the browser, on your laptop)

1. In Databricks, click your avatar (top right) → **Settings → Developer → Access tokens → Manage → Generate new token**.
2. Comment `pitlake-collector-vm`, lifetime **90 days**.
3. Copy the token (it starts with `dapi`). It is shown only once.
4. Add a calendar reminder about 80 days from now to rotate it (repeat V9 and V10).

### V10. Store the credentials and configuration on the VM

```bash
sudo install -d -m 755 /etc/pitlake
sudo install -m 600 /dev/null /etc/pitlake/collector.env
sudo nano /etc/pitlake/collector.env
```

Type these two lines, pasting your token:

```
DATABRICKS_HOST=https://dbc-cc51bd40-0056.cloud.databricks.com
DATABRICKS_TOKEN=dapi...your token...
```

Save with **Ctrl+O**, **Enter**, then exit with **Ctrl+X**. Then:

```bash
sudo cp /opt/pitlake/collector/config.prod.toml /etc/pitlake/collector.toml
sudo ls -l /etc/pitlake
```

✅ `collector.env` shows `-rw-------  root root`.

### V11. Test run: land 3 days in prod

```bash
sudo systemd-run --uid=pitlake --gid=pitlake \
  -p EnvironmentFile=/etc/pitlake/collector.env -p StateDirectory=pitlake-collector \
  --pty --wait --collect \
  /opt/pitlake/.venv/bin/pitlake-collector sync --config /etc/pitlake/collector.toml --end 2025-01-03
```

✅ Three `landed` lines, then `"feed synced"` with `"landed": 3, "failed": 0`.
❌ Errors that mention `401`, `403` or `invalid access token` mean the host or token in V10 is wrong. Fix the file and repeat V11.

From the **laptop**, confirm the files arrived:

```bash
databricks fs ls dbfs:/Volumes/pitlake_prod/raw/landing/binance/spot_trades/BTCUSDT --profile pitlake
```

✅ 6 entries.

### V12. Install the service. It starts the full-year backfill.

```bash
sudo cp /opt/pitlake/collector/deploy/pitlake-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pitlake-collector
systemctl status pitlake-collector --no-pager
```

✅ `Active: active (running)`. Now watch it work (press Ctrl+C to stop watching; the service keeps running):

```bash
journalctl -u pitlake-collector -f -o cat
```

You'll see one `landed` line per day of 2025. The backfill of the remaining 362 files takes roughly
30 to 90 minutes. It is finished when a line shows `"event": "feed synced"` with `"failed": 0`.
If some files failed, the next cycle an hour later retries them automatically; wait for a
`feed synced` line with `"missing": 0`.

You can disconnect (`exit`) at any time; the service keeps running and restarts after reboots.

### V13. Keep the VM from being reclaimed (recommended)

Oracle may reclaim Always Free instances that stay idle for 7 days, and this collector is idle most
of the time. To prevent it, upgrade the account in **☰ → Billing → Upgrade and manage payment →
Pay As You Go**. Always Free resources stay free after upgrading. If the VM is ever reclaimed,
nothing is lost, because the landing volume holds all the state. Repeat this appendix to rebuild it.

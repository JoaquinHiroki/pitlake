# Stage 1 setup runbook

Follow the parts in order. Each step says what you should see. If you see something else, stop and
keep the full output; don't continue to the next step.

- **Part A** (laptop, about 45 minutes): fix the local environment, deploy to dev, prove the pipeline end to end in dev.
- **Part B** (Oracle Cloud, about 1 hour): create the VM and install the collector.
- **Part C** (laptop, spread over one or two days because of compute quota): load one year into prod and prove the Stage 1 exit test.

---

## Part A: laptop and dev

### A1. Move the repository out of the iCloud Desktop

Your Desktop is synced by iCloud, which keeps hiding files inside `.venv`. Python then can't find the
project's own packages (`ModuleNotFoundError: No module named 'pitlake_collector'`).

1. Close the VS Code window that has PITLake open.
2. Open the macOS **Terminal** app (not the VS Code terminal) and run:

   ```bash
   mkdir -p ~/code
   mv ~/Desktop/PITLake ~/code/PITLake
   cd ~/code/PITLake
   rm -rf .venv
   make setup
   ```

3. Check that the environment is healthy:

   ```bash
   ls -lO .venv/lib/python3.12/site-packages/*.pth
   ```

   ✅ Every line shows `-` in the flags column, not `hidden`.

4. Open the folder in VS Code again: **File → Open Folder… → ~/code/PITLake**.

From here on, run every command from `~/code/PITLake`.

### A2. Run lint and tests

```bash
make check
```

✅ The last line is `74 passed`.

### A3. Try the collector locally (no Databricks)

```bash
make collector-try
```

✅ The last line is a JSON line with `"event": "feed synced"`, `"landed": 2` and `"failed": 0`.

```bash
find data -type f      # 4 files: two .zip, two .zip.manifest.json
rm -rf data
```

### A4. Commit and push

```bash
git add -A
git status
```

Check the list: it should contain `collector/`, `src/pitlake/...`, `docs/...`, `resources/...` and
config files. It must **not** contain `data/`, `.env` files or anything with a token.

```bash
git commit -m "Stage 1: Binance collector, landing manifests, idempotent Bronze load"
git push
```

Then open https://github.com/JoaquinHiroki/pitlake/actions.
✅ The newest **CI** run turns green in about a minute.

### A5. Deploy to dev

```bash
make validate
```

✅ `Validation OK!`
❌ If you see an authentication error, log in again and repeat:
`databricks auth login --host https://dbc-cc51bd40-0056.cloud.databricks.com --profile pitlake`

```bash
make deploy
```

✅ Ends with `Deployment complete!`

Check in the Databricks UI:
- **Catalog → pitlake_dev → bronze** has a volume `staging`, and **control** has a volume `checkpoints`.
- **Jobs & Pipelines** lists `pitlake-ingest-dev` (schedule shown as paused).

### A6. Smoke test

```bash
make smoke
```

✅ The run finishes with state `SUCCESS`.

### A7. Land the dev sample from your laptop

This uses your `pitlake` login and uploads 7 days of BTCUSDT (about 100 MB) into `pitlake_dev`.

```bash
make collector-dev
```

✅ Seven `"event": "landed"` lines, then `"feed synced"` with `"landed": 7, "failed": 0`.

```bash
databricks fs ls dbfs:/Volumes/pitlake_dev/raw/landing/binance/spot_trades/BTCUSDT --profile pitlake
```

✅ 14 entries: 7 `.zip` files and 7 `.zip.manifest.json` files.

```bash
rm -rf data
```

### A8. Run the ingest job in dev

```bash
make ingest
```

The command prints a **Run URL** and waits until the job finishes (a few minutes).

✅ Both tasks, `discover_manifests` and `load_bronze`, end `SUCCESS`. Open the Run URL, click each task, and check its output:
- `discover_manifests`: `OK: discovered 7 new manifests in pitlake_dev`
- `load_bronze`: `OK: binance.spot_trades loaded 7 files, <N> rows`

❌ If a task fails, open it, copy the whole error (the red box and the stack trace below it) and send it to Claude. These two jobs had never run on Databricks before this step.

### A9. Check the data in dev

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

✅ `files` = 7, `row_count` = `unique_trades`, `first_day` = 2025-01-01, `last_day` = 2025-01-07, `corrupt_rows` = 0.
Write down `row_count`.

### A10. Prove that a reload adds no duplicates (dev rehearsal of the exit test)

```bash
databricks bundle run ingest -t dev --params reload=true,period_start=2025-01-01,period_end=2025-01-07
```

✅ `load_bronze` output says `loaded 7 files`. Run the A9 query again:
**every number is identical**, including `row_count`.

```sql
SELECT landing_path, count(*) AS loads
FROM pitlake_dev.control.bronze_load_log
GROUP BY landing_path ORDER BY landing_path;
```

✅ 7 rows with `loads` = 2. The log keeps the history of loads, while the Bronze table keeps a single copy of the rows.

### A11. Deploy to prod

```bash
make deploy-prod
databricks bundle run smoke -t prod
```

✅ `Deployment complete!`, then the smoke run ends `SUCCESS`.

Part A is done. The dev pipeline is proven and prod is ready to receive files.

---

## Part B: Oracle Cloud VM

### B1. Create the Oracle Cloud account

1. Go to https://signup.cloud.oracle.com and choose **Start for free**.
2. Fill in the form. **Home Region cannot be changed later.** Pick a region near you that is **not
   in the United States** (Binance restricts US traffic). Step B5 confirms the choice works.
3. Oracle verifies a credit card with a small temporary hold. Always Free resources are not charged.
4. Wait for the "account is ready" email (a few minutes, occasionally longer), then sign in at
   https://cloud.oracle.com.

### B2. Create an SSH key on your laptop

```bash
ssh-keygen -t ed25519 -f ~/.ssh/pitlake_oci -C pitlake-vm
```

Press Enter at the passphrase prompts, or set a passphrase you will remember. Then copy the public key:

```bash
pbcopy < ~/.ssh/pitlake_oci.pub
```

### B3. Create the instance

In the Oracle console: **☰ menu → Compute → Instances → Create instance**. Labels may differ
slightly from these, but the choices are the same.

| Setting | Value |
|---|---|
| Name | `pitlake-collector` |
| Image | **Change image → Ubuntu → Canonical Ubuntu 24.04** (the standard image, not "Minimal") |
| Shape | **Change shape → Ampere → VM.Standard.A1.Flex**, 1 OCPU, 6 GB memory. It must show the *Always Free-eligible* label |
| Networking | Create new virtual cloud network and a new **public** subnet, and turn on **Automatically assign public IPv4 address** |
| SSH keys | **Paste public keys**, then paste (⌘V) the key you copied in B2 |
| Boot volume | Leave the defaults |

Click **Create**.

❌ If you get **"Out of capacity for shape VM.Standard.A1.Flex"**, change the shape to **Specialty and
previous generation → VM.Standard.E2.1.Micro** (also Always Free) and click Create again.

✅ After 1 to 2 minutes the state is **Running**. Copy the **Public IP address** from the instance page.

### B4. Connect

```bash
ssh -i ~/.ssh/pitlake_oci ubuntu@<PUBLIC_IP>
```

Type `yes` to the fingerprint question. ✅ The prompt becomes `ubuntu@pitlake-collector:~$`.
**Every command in B5 to B12 runs on the VM.**

### B5. Check that Binance is reachable from this region

```bash
curl -s https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip.CHECKSUM
```

✅ `088c0401c491b235bf33ef5884633b2f080c9ba6afb8231f11fd71cea6e2d93c  BTCUSDT-trades-2025-01-01.zip`
❌ If you get an error page or a 403/451 code, the region is blocked. Terminate the instance and use
another region. Ask Claude before doing this.

### B6. Update the system

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y git curl
python3 --version
```

If a purple "Daemons using outdated libraries" screen appears, press Enter.
✅ `Python 3.12.x`. If the upgrade installed a new kernel, run `sudo reboot`, wait 1 minute and reconnect as in B4.

### B7. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
uv --version
```

✅ Prints `uv 0.x.y`.

### B8. Create the service user and install the collector

```bash
sudo useradd --system --create-home --home-dir /var/lib/pitlake --shell /usr/sbin/nologin pitlake
sudo install -d -o pitlake -g pitlake /opt/pitlake
sudo -u pitlake -H git clone https://github.com/JoaquinHiroki/pitlake.git /opt/pitlake
cd /opt/pitlake
sudo -u pitlake -H uv sync --package pitlake-collector --no-dev --frozen
/opt/pitlake/.venv/bin/pitlake-collector --version
```

✅ The last command prints `0.1.0`.
❌ If `git clone` finds no `collector/` directory, step A4 (the push) did not happen.

### B9. Create a Databricks token for the VM (in the browser, on your laptop)

1. In Databricks, click your avatar (top right) → **Settings → Developer → Access tokens → Manage → Generate new token**.
2. Comment `pitlake-collector-vm`, lifetime **90 days**.
3. Copy the token (it starts with `dapi`). It is shown only once.
4. Add a calendar reminder about 80 days from now to rotate it (repeat B9 and B10).

### B10. Store the credentials and configuration on the VM

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

### B11. Test run: land 3 days in prod

```bash
sudo systemd-run --uid=pitlake --gid=pitlake \
  -p EnvironmentFile=/etc/pitlake/collector.env -p StateDirectory=pitlake-collector \
  --pty --wait --collect \
  /opt/pitlake/.venv/bin/pitlake-collector sync --config /etc/pitlake/collector.toml --end 2025-01-03
```

✅ Three `landed` lines, then `"feed synced"` with `"landed": 3, "failed": 0`.
❌ Errors that mention `401`, `403` or `invalid access token` mean the host or token in B10 is wrong. Fix the file and repeat B11.

From the **laptop**, confirm the files arrived:

```bash
databricks fs ls dbfs:/Volumes/pitlake_prod/raw/landing/binance/spot_trades/BTCUSDT --profile pitlake
```

✅ 6 entries.

### B12. Install the service. It starts the full-year backfill.

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

### B13. Keep the VM from being reclaimed (recommended)

Oracle may reclaim Always Free instances that stay idle for 7 days, and this collector is idle most
of the time. To prevent it, upgrade the account in **☰ → Billing → Upgrade and manage payment →
Pay As You Go**. Always Free resources stay free after upgrading. If the VM is ever reclaimed,
nothing is lost, because the landing volume holds all the state. Repeat Part B to rebuild it.

---

## Part C: load the year into prod and pass the exit test

Run these on the **laptop**, from `~/code/PITLake`. You can start while the backfill (B12) is still
running; each job run loads whatever has landed so far.

### C1. Load in chunks

```bash
databricks bundle run ingest -t prod
```

Each run loads up to 30 files (the `max_files` default). Open the Run URL, go to the `load_bronze`
output and note how long the run took. Repeat the command until the output says
`binance.spot_trades: 0 files to load` **and** the collector has finished (B12).

- If runs are quick (under about 15 minutes), load more per run: `databricks bundle run ingest -t prod --params max_files=90`
- ❌ If a run fails with a message about the **compute quota** being exceeded, stop for the day and continue tomorrow. Progress is saved after every 5 files, so nothing is redone.

### C2. Verify the year

In the SQL Editor:

```sql
SELECT count(*)                     AS row_count,
       count(DISTINCT trade_id)     AS unique_trades,
       count(DISTINCT _source_file) AS files,
       min(_period_start)           AS first_day,
       max(_period_start)           AS last_day,
       count(_corrupt_record)       AS corrupt_rows
FROM pitlake_prod.bronze.binance_spot_trades;
```

✅ `files` = 365, `first_day` = 2025-01-01, `last_day` = 2025-12-31, `row_count` = `unique_trades`, `corrupt_rows` = 0.
**Save a screenshot of this result.** It is your evidence for the first half of the exit test.

### C3. The exit test: reload a period, prove no duplicates

```bash
databricks bundle run ingest -t prod --params reload=true,period_start=2025-03-01,period_end=2025-03-31
```

✅ `load_bronze` says `loaded 31 files`. Run the C2 query again: **all six numbers are identical**.
Save this second screenshot. **Stage 1 is complete.**

Send both screenshots and the two Run URLs to Claude to record Stage 1 as done.

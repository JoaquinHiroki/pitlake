# Deploy PITLake from scratch

This guide takes a new Databricks Free Edition workspace to a running PITLake: five sources
collected every day, Bronze, Silver and Gold rebuilt and audited, backtests, the operations job,
monitoring, the console app and two Genie spaces. It assumes no earlier knowledge of the project.
The stage runbooks in this folder and the ADRs in [docs/adr](../adr) explain why each part works
the way it does; you do not need them to deploy.

Every step says what you should see (✅). If you see something else, stop, keep the full output,
and look it up in [Troubleshooting](#troubleshooting) before going on. All times are UTC.

| Part | What it does | Time |
|---|---|---|
| [A](#part-a-accounts-and-laptop) | Accounts, keys and laptop tools | 30 min |
| [B](#part-b-dev) | Deploy dev and fill it with a fixed two-week sample | 1 to 2 h |
| [C](#part-c-the-console-and-genie) | The console app and the Genie spaces | 20 min |
| [D](#part-d-prod) | Deploy prod, load the history, switch on the schedules | a few days, quota-bound |
| [E](#part-e-optional-retrieval-and-fine-tuning) | Optional: filing retrieval and the fine-tuned generator | 2 to 3 h |

---

## Part A: accounts and laptop

### A1. Accounts and keys

| What | Where | Used for |
|---|---|---|
| Databricks Free Edition workspace | databricks.com/learn/free-edition | Everything |
| FRED API key | fred.stlouisfed.org → My Account → API Keys | Macro series |
| Alpaca paper-trading keys (key ID and secret) | alpaca.markets → Paper account → API keys | US stock trades and bars |
| An email address | Your own | The SEC asks every client to name a contact in its User-Agent |

Binance and Coinbase need no account.

### A2. Laptop tools

macOS or Linux with `git`, and:

```bash
brew tap databricks/tap && brew install databricks
curl -LsSf https://astral.sh/uv/install.sh | sh
databricks --version
```

✅ `Databricks CLI v1.17.0` or later. Earlier versions do not know the Genie and GPU resources.

### A3. Get the code

Clone into a folder that iCloud or another sync service does not touch (not Desktop or
Documents on a Mac with iCloud Drive): sync services hide files inside `.venv` and Python stops
finding the project's packages.

```bash
mkdir -p ~/code && cd ~/code
git clone https://github.com/JoaquinHiroki/pitlake.git PITLake
cd PITLake
make setup
make check
```

✅ `All checks passed!`, then `430 passed, 5 skipped` (or more passed, if tests were added since).
Run every later command from this folder.

### A4. Log in

Copy your workspace URL from the browser (it looks like `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`):

```bash
databricks auth login --host <your workspace URL> --profile pitlake
echo 'export DATABRICKS_CONFIG_PROFILE=pitlake' >> ~/.zshrc
export DATABRICKS_CONFIG_PROFILE=pitlake
databricks current-user me
```

✅ A browser tab opens, the terminal prints `Profile pitlake was successfully saved`, and the last
command prints your user. The variable makes every `databricks` and `make` command use this
workspace (use `~/.bashrc` instead if your shell is bash). If a later command fails with an
authentication error, repeat the first command; the login expires after a while.

### A5. Catalogs and secrets

```bash
make bootstrap
databricks secrets create-scope pitlake
databricks secrets put-secret pitlake fred-api-key --string-value '<FRED key>'
databricks secrets put-secret pitlake alpaca-key-id --string-value '<Alpaca key ID>'
databricks secrets put-secret pitlake alpaca-secret-key --string-value '<Alpaca secret>'
databricks secrets put-secret pitlake sec-user-agent --string-value 'PITLake <your email>'
databricks secrets list-secrets pitlake
```

✅ `created: pitlake_dev` and `created: pitlake_prod` (the first may take a minute while the
Serverless Starter Warehouse starts: the catalogs are created with SQL, which Free Edition puts on
its Default Storage), then four keys listed: `alpaca-key-id`,
`alpaca-secret-key`, `fred-api-key`, `sec-user-agent`. The jobs read them at run time; they never
appear in the code or the logs.

---

## Part B: dev

Dev collects a fixed window: two weeks of January 2025 for the crypto venues, the first trading
days of January 2025 for SPY and AAPL, and the first quarter of 2025 for FRED and EDGAR. The
numbers below are what that window always produces.

### B1. First deploy

```bash
make validate
make deploy
```

✅ `Validation OK!`. The deploy creates the schemas, volumes, jobs, experiments and alerts, and
on a new catalog it then **ends with three errors**, on the two Genie spaces and the console app:

```
Error: cannot create resources.genie_spaces.operations_genie: An error occurred accessing the schema. Failed to fetch tables for the agent. ... 'pitlake_dev.control.backtest_runs' does not exist, ... (403 PERMISSION_DENIED)
Error: cannot create resources.genie_spaces.markets_genie: An error occurred accessing the schema. Failed to fetch tables for the agent. ... 'pitlake_dev.gold.bars_1d' does not exist, ... (403 PERMISSION_DENIED)
Error: cannot create resources.apps.console: dependency failed: resources.genie_spaces.operations_genie
```

That is expected (the message says `Catalog`, `Schema` or `Table` before each name, depending on
what exists yet). The Genie spaces and the app read tables that the jobs create on their first
runs, so they can only be deployed after B2 to B4 (C1 deploys them). No other resource may fail:
**Jobs & Pipelines** lists `pitlake-ingest-dev`, `pitlake-transform-dev`,
`pitlake-operations-dev` and the others. An error on `resources.schemas` or `resources.volumes`
means the catalog is missing; run A5's `make bootstrap` and deploy again.

### B2. Collect and load

```bash
databricks bundle run ingest -t dev --params max_files=0
```

The command prints a **Run URL** and waits (about 10 minutes). ✅ `TERMINATED SUCCESS`. The
`plan` task prints `8 datasets registered`, every `collect` and `load_bronze` iteration succeeds,
and `commit` ends `committed 8 datasets in pitlake_dev`.

If the terminal loses its connection while waiting, the run keeps going. Check it with
`databricks jobs get-run <run ID>` (the ID is the number at the end of the Run URL).

### B3. Clean, compare and build Gold

```bash
make transform
```

About 10 minutes. ✅ `TERMINATED SUCCESS`, with the tasks `plan → transform → gate → commit →
gold → commit_gold`. `gate` ends `OK: quality gate passed ... checks`, and `gold` ends
`OK: Gold built and audited in pitlake_dev: no leakage in 12 checks`.

In Databricks, open **SQL Editor**, choose **Serverless Starter Warehouse**, and run:

```sql
SELECT silver_table, sum(rows_in) AS judged, sum(rows_out) AS kept,
       sum(rows_quarantined) AS quarantined, count(*) AS files
FROM (SELECT * FROM pitlake_dev.control.silver_load_log
      QUALIFY row_number() OVER (PARTITION BY silver_table, source_file
                                 ORDER BY processed_at DESC) = 1)
GROUP BY silver_table ORDER BY silver_table;
```

✅ These rows; `kept` + `quarantined` = `judged` on every row:

| silver_table | judged | kept | quarantined | files |
|---|---|---|---|---|
| alpaca_stock_bars_1d | 12 | 12 | 0 | 12 |
| alpaca_stock_trades | 151816 | 151816 | 0 | 12 |
| binance_spot_trades | 49675597 | 49675597 | 0 | 14 |
| coinbase_spot_candles_1m | 20160 | 20160 | 0 | 14 |
| edgar_company_facts | about 58000 | all | 0 | 2 |
| edgar_filing_documents | about 10 | all | 0 | about 10 |
| edgar_filings | about 2000 | all but about 10 | about 10 | 2 |
| fred_series_vintages | 8127 | 8127 | 0 | 9 |

The EDGAR counts depend on what the SEC publishes on the day you collect. The quarantined filings
are SEC "NO ACT" and "EFFECT" notices without a plausible time stamp
([ADR 0006](../adr/0006-silver-quality-rules.md)).

### B4. Backtests and operations

```bash
databricks bundle run research -t dev --params start=2025-01-02
databricks bundle run operations -t dev
```

✅ Both `TERMINATED SUCCESS`. Research prints one line per backtest and `OK: ... 24 backtests`.
Operations runs six tasks; `monitors` takes about 10 minutes the first time because it creates a
Lakehouse Monitoring monitor for each Silver table. `freshness` prints `STALE` for most tables:
dev holds a window from 2025 on purpose, and in dev that is recorded, not enforced.

---

## Part C: the console and Genie

### C1. Second deploy and start the app

```bash
make deploy
databricks bundle run console -t dev
```

✅ The deploy ends without an error and creates the two Genie spaces and the console app
(`Created genie_spaces.markets_genie`, `Created genie_spaces.operations_genie`,
`Created apps.console`), and the run ends
`App started successfully` with a URL like `https://pitlake-console-dev-<number>.aws.databricksapps.com`.

Open the URL. ✅ Seven tabs: **Coverage** (tables, periods and freshness), **Rejections**, **Crypto
venues**, **Backtests**, **Performance**, **Ask** and **Ingest**. Free Edition stops an app 24
hours after it starts; run the second command again to start it.

### C2. Ask Genie

In the console's **Ask** tab, choose **Markets** and ask
`What was Apple's latest quarterly revenue as known on 2024-08-02?`

✅ About $85,777 million, the quarter to 2024-06-29, with the SQL Genie ran. The SQL filters on
`knowable_at <= TIMESTAMP'2024-08-02 00:00:00'`: the markets space only answers with what was
public at the instant asked for ([ADR 0014](../adr/0014-genie-spaces.md)). The same spaces are in
the Databricks sidebar under **Genie**.

### C3. Benchmark the spaces

```bash
make genie-benchmarks TARGET=dev
```

About 3 minutes. ✅ `PITLake markets · dev: 5 correct ... of 5` and
`PITLake operations · dev: 4 correct ... of 4`, each followed by the questions and Genie's SQL.

---

## Part D: prod

Prod collects from 2025-01-01 (EDGAR from 2019) to yesterday and keeps going every day. Its
first load is large: about 2.4 billion Binance trades. Free Edition limits how much serverless
compute a day can use, so the history arrives over several days, and nothing is lost when a run
stops at the limit.

### D1. Deploy

```bash
make deploy-prod
```

✅ The same as B1, including the three expected errors on the Genie spaces and the console app.

### D2. Load the history

```bash
databricks bundle run ingest -t prod --params max_files=120
```

Repeat it, at most a few times a day, until a run's `load_bronze` iterations all print
`0 files to load`. Each successful run commits, and the commit starts `pitlake-transform-prod`
by itself (prod's transform listens for new commits). Check that the transforms succeed in
**Jobs & Pipelines → pitlake-transform-prod**.

❌ A run that fails with a compute-quota message: stop for the day and repeat tomorrow.
❌ `collect` with `"failed"` above 0: run the same command again; failed files are retried.

The quality gate compares Binance and Coinbase minute by minute. On three days of 2025 and 2026
the venues really did disagree or Coinbase was down (2025-10-10, 2025-10-25, 2026-05-08), so the
first prod transform that reaches them fails on purpose. Record them as known and rerun, as
[stage 4's runbook](stage4-quality-gate.md) describes in its part B.

### D3. Operations, research and the console

```bash
databricks bundle run operations -t prod
databricks bundle run research -t prod
make deploy-prod
databricks bundle run console -t prod
make genie-benchmarks TARGET=prod
```

✅ All succeed, the deploy now without an error, and the benchmarks print 5 of 5 and 4 of 4.

### D4. What runs by itself from now on

The prod target switches the schedules on:

| When | Job | What it does |
|---|---|---|
| 06:30 daily | `pitlake-ingest-prod` | Collects yesterday's files and loads them into Bronze |
| After each ingest commit | `pitlake-transform-prod` | Silver, the quality gate, Gold and its leakage audit |
| 10:00 daily | `pitlake-operations-prod` | Costs, query statistics, table health, freshness, monitors, drift |
| 11:30 daily | Alerts `pitlake-silver-drift-prod`, `pitlake-operations-heartbeat-prod` | Email you if a Silver column drifted or the checks stopped running |

A failed job emails the workspace owner. The console shows the state at any time.

---

## Part E: optional retrieval and fine-tuning

- **Point-in-time retrieval over 10-K and 10-Q text** (`make rag`, [stage 7's runbook](stage7-rag.md)).
  The first run creates the AI Search endpoint `pitlake`. It keeps running, and using your compute
  quota, until you delete it under **Compute → AI Search**; Free Edition allows one per account,
  which dev and prod share.
- **The fine-tuned answer generator** needs serverless GPU, which Free Edition opens only after
  you verify your identity (the **Verify identity** button in the workspace). Then follow part D of
  [stage 7's runbook](stage7-rag.md).
- **The fine-tuned reranker**, which reorders the retriever's top 20 passages, needs the same GPU
  access and a tuned retriever. Follow [stage 9's runbook](stage9-reranker.md).

---

## Free Edition limits that shape the design

| Limit | Effect |
|---|---|
| Daily serverless compute quota | Backfills run in chunks (`max_files`); a stopped run resumes |
| 3 apps per account, 20 resources per app | One console per environment; the console reads 15 tables |
| An app stops 24 hours after it starts | Start it with `databricks bundle run console -t <target>` |
| One AI Search endpoint per account | Dev and prod share `pitlake` |
| Genie conversation API, about 5 questions a minute | The Ask tab is for people, not scripts |
| GPU only after identity verification | Part E only |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` for `pitlake` or `pitlake_collector` on the laptop | The repository is in an iCloud-synced folder | Move it (A3), `rm -rf .venv`, `make setup` |
| `Failed to fetch tables for the agent ... does not exist (403)` on `genie_spaces`, `dependency failed` on `apps.console` | First deploy of a target (B1, D1) | Run the jobs first, then deploy again |
| `Catalog 'pitlake_<target>' does not exist (404)` on `schemas` | The catalog was not created | `make bootstrap` (A5), then deploy again |
| `Number of app resources must be less than or equal to 20` | A table was added to a Genie space or the console | Remove one; `tests/test_console.py` checks the count |
| Authentication errors | The CLI login expired | Repeat A4 |
| The CLI stops while waiting for a run | The connection dropped; the run continues | `databricks jobs get-run <run ID>` |
| `collect` fails for one source with HTTP 401 or 403 | A secret is missing or wrong | Check `databricks secrets list-secrets pitlake` and put it again (A5) |
| `REQUEST_LIMIT_EXCEEDED` in the rag job | Free Edition's embedding rate limit | Already handled with small batches; rerun later |
| Transform fails in `gate` on a real day | The venues disagreed or one was down | Look at the day in the console's Crypto venues tab; waive it if it is a market event (stage 4's runbook) |

## Removing everything

```bash
databricks bundle destroy -t dev
databricks bundle destroy -t prod
databricks catalogs delete pitlake_dev --force
databricks catalogs delete pitlake_prod --force
```

Then delete the AI Search endpoint `pitlake` if Part E created it.

## Appendix: rehearsing this guide in a workspace that already runs PITLake

The `rehearsal` target is a third copy with its own catalog, `pitlake_rehearsal`, the dev window
and every schedule paused. It proves the guide on empty tables without touching dev or prod.
Follow parts B and C with these substitutions, then remove it:

- `make bootstrap` → `uv run python scripts/bootstrap.py pitlake_rehearsal`; skip the secrets, which already exist
- `make validate` → `databricks bundle validate -t rehearsal`
- `make deploy` → `databricks bundle deploy -t rehearsal`
- `-t dev` → `-t rehearsal`, `make transform` → `databricks bundle run transform -t rehearsal --params max_files=0`
- `pitlake_dev` → `pitlake_rehearsal` in SQL, `TARGET=dev` → `TARGET=rehearsal`

```bash
make destroy-rehearsal
databricks catalogs delete pitlake_rehearsal --force
```

It uses the account's third and last app while it exists.

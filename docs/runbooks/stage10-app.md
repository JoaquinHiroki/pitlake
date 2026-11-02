# Stage 10 runbook: the console app

Replaces the Streamlit console with a FastAPI backend and a React frontend centred on the two
Genie spaces ([ADR 0019](../adr/0019-console-web-app.md)). The bundle resource, its service
principal and its grants are unchanged.

Before you start: the target has been deployed with its Genie spaces (deploy guide, part C), and
Node.js 20 or newer is installed locally (`node --version`).

## Part A: local

### A1. Backend

```bash
make console-api TARGET=dev
```

✅ `Uvicorn running on http://127.0.0.1:8000`. In another terminal,
`curl -s localhost:8000/api/config` prints `"catalog":"pitlake_dev"` and the two spaces. It runs
as you, through the `pitlake` profile, with the ids from `databricks bundle summary`.

### A2. Frontend

```bash
make console-web
```

✅ Vite prints `Local: http://localhost:5173/`. Open it: the **Ask** page with the Markets space
and three starter questions. Click
`What was Apple's latest quarterly revenue as known on 2024-08-02?`

✅ The reply shows Genie's stages (reading the space, writing the query, running the query), then
the result row AAPL, period_end 2024-06-29, value 85,777,000,000, and the SQL under **SQL** with
`knowable_at <= TIMESTAMP'2024-08-02 00:00:00'`. Ask `And Microsoft?` as a follow-up: ✅ the same
conversation answers for MSFT. **Earlier conversations** lists the new one.

Stop both with Ctrl+C.

## Part B: dev

### B1. Deploy and start

```bash
make deploy
databricks bundle run console -t dev
```

✅ The deploy updates `apps.console`, and the run ends `App started successfully`. The app's
deployment log (Compute → Apps → `pitlake-console-dev` → Logs) shows `npm install`, the
`vite build` output and `Uvicorn running on http://0.0.0.0:8000`.

❌ `vite: not found` or `tsc: not found`: a build tool moved to `devDependencies`; it belongs in
`dependencies` (ADR 0019).

### B2. Check every page

Open the app URL.

- ✅ **Ask**: repeat A2 as the app's service principal. The answer and SQL are the same.
- ✅ **Coverage**: freshness (6 of 8 stale in dev), data per source, and rows per period.
- ✅ **Rejections**, **Crypto venues**, **Backtests** (click a run for its versions and hashes),
  **Performance**: tables, each with a working **Refresh**.
- ✅ **Ingest**: the 8 datasets and the recent runs. Start nothing unless you mean to.

## Part C: prod

```bash
make deploy-prod
databricks bundle run console -t prod
```

✅ As B1 and B2, with prod data: 0 stale tables, and AAPL's revenue answer unchanged.

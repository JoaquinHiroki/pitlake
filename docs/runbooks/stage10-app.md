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

A1 keeps running, so open a second terminal:

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

❌ `Waiting for the pending deployment to complete...` then `app crashed unexpectedly`, and the
app's logs show `Starting app with command: [streamlit run app.py]`. A stopped app is first
restarted with its last successful deployment's command, which in prod is still the Streamlit
console's; that start fails and the run stops before deploying the new command. The compute is
running now, so run `databricks bundle run console -t prod` again: it deploys `python server.py`.

## Part D: digests and charts

Adds the **Digest** page, charts for Genie's time series and Markdown in Genie's text
([ADR 0020](../adr/0020-point-in-time-digests.md)). The app gains one resource, the chat endpoint,
which makes 20 of 20.

### D1. Local

```bash
make console-api TARGET=dev
```

In a second terminal:

```bash
make console-web
```

Open `http://localhost:5173/#/digest`.

- ✅ AAPL is chosen, the as-of is the day after its last dev session, and the line says
  `Gold holds AAPL sessions 2025-01-02 to 2025-01-10`.
- Set the as-of to 2025-01-08, 00:00. ✅ Last close known 245.00 on 2025-01-06: the 01-07 bar
  became public at 05:00 UTC on 01-08. **Known at the instant** shows Revenue, quarter $85,777M
  for 2024-06-29 (the number Genie gives in A2), and **Filings** shows the 10-Q for 2024-12-28 in
  grey, `not yet public at the instant`.
- Click **Write the digest**. ✅ A few sentences that cite the 10-K for 2024-09-28, and under them
  the facts it was given, none dated after 2025-01-08 00:00.
- Choose BTCUSDT, as-of 2025-01-07, and tick **Reveal**. ✅ The line is solid up to 01-06
  (102,235.60) and faint after it, over a shaded band.
- On **Ask**, ask the Markets space `Daily closes of BTCUSDT on binance in January 2025`.
  ✅ The result opens as a line chart with **Chart** and **Table** buttons, and bold text in
  Genie's reply shows as bold, not `**`.

### D2. Dev

```bash
make deploy
databricks bundle run console -t dev
```

✅ `App started successfully`. In the app's **Authorization** tab, the resources include
`chat-endpoint` with `Can query`. Repeat D1 in the deployed app.

❌ The deploy fails on `chat-endpoint`: the workspace refused the grant on the system endpoint.
Remove the `CHAT_ENDPOINT` entry and the `chat-endpoint` resource from `resources/console.yml` and
deploy again. The Digest page then shows the facts and says it cannot write the digest; everything
else is unchanged.

### D3. Prod

```bash
make deploy-prod
databricks bundle run console -t prod
```

✅ As D2, with prod data from 2025-01-01. Good instants for a demonstration:

- AAPL as of 2025-07-31 and then 2025-08-04: Revenue, quarter moves from the quarter ending
  2025-03-29 to the one ending 2025-06-28 when the 10-Q is accepted. **Filings** gives the
  acceptance time, so an instant just before and just after it shows the switch exactly.
- BTCUSDT as of 2025-10-10 with **Reveal** ticked: the crash after the instant appears only in the
  shaded part, and the written digest knows nothing of it.
- On **Ask**, `Compare the daily closes of AAPL and NVDA in October 2025`: two lines, a legend and
  end labels.

## Part E: portfolio digest

Adds the **Portfolio** page ([ADR 0020](../adr/0020-point-in-time-digests.md)). No resource or
grant changes.

### E1. Local

With `make console-api TARGET=dev` and `make console-web` running, open
`http://localhost:5173/#/portfolio`.

- ✅ Four starter positions: AAPL 10, NVDA 20, SPY 5, BTCUSDT 0.05, as of 2025-01-15 00:00.
  `No close of NVDA was public at the instant`: dev holds no NVDA bars.
- Set the as-of to 2025-01-08, 00:00. ✅ Value known $10,274.53 from the closes of 2025-01-07,
  1D −2.51%, and positions AAPL $2,450.00 (23.8%), SPY $2,976.80 (29.0%) and BTCUSDT $4,847.73
  (47.2%).
- Click **Write the digest**. ✅ It puts the fall down to bitcoin's −5.17% at 47.2% of the
  portfolio, and its facts end with the fed funds rate public since 2025-01-03.
- Change a quantity and click **Update portfolio**. ✅ The value and weights follow; reload the
  page and the holdings are still there.

### E2. Dev and prod

```bash
git push
databricks bundle run console -t dev
```

✅ CI deploys dev, then `App started successfully`; repeat E1 in the dev app. Then:

```bash
make deploy-prod
databricks bundle run console -t prod
```

✅ `App started successfully`. With prod data, as of 2025-10-10 and then 2025-10-11 with
**Reveal**: the October crash moves the value only after the instant.

# Console runbook: the Results page

The console's **Results** page publishes the attribution, sufficiency and data-value results from
views in the `results` schema, which the bundle grants to the app
([ADR 0033](../adr/0033-console-results.md)).

Before you start: the target has been deployed with its console app (deploy guide, part C), and
Node.js 20 or newer is installed locally.

## Part A: dev

### A1. Test and deploy

```bash
make check
make validate
make deploy
```

✅ `Validation OK!`, and the deploy ends without an error. **Catalog** shows a `results` schema in
`pitlake_dev`, with `USE SCHEMA` and `SELECT` granted to the app's service principal under
**Permissions**.

### A2. Create the views

```bash
make smoke
```

✅ `TERMINATED SUCCESS`, ending `OK: pitlake_dev has all layers, landing volume, a writable control
schema and the published results views`. The `results` schema now holds `attribution_points`,
`attributions`, `sufficient_sets` and `source_values`.

### A3. The page, run locally

```bash
make console-api TARGET=dev
```

In a second terminal:

```bash
make console-web
```

Open `http://localhost:5173/#/results`. ✅ Three sections, for backtest `0fc8f747`:

- **Which information caused each decision**: two decisions at 2025-01-15 06:00. The agent has
  131 worlds, 127 screened, 3 causing, `null_changes_nothing` true; the strategy 146 worlds,
  0 causing, `null_changes_nothing` true.
- Choose the agent's row: **Items that change the agent decision** lists `bar:AAPL/2025-01-10`,
  `bar:BTCUSDT/2025-01-14` and `bar:SPY/2025-01-10`, each flipping its own symbol with
  p 0.0119.
- **The least information that reproduces each decision**: the strategy, 146 candidates,
  `kept_count` 0, confirmed and coherent. Choose it: `No item: it holds with none.`
- **Which sources change decisions**: 4 rows for the agent (binance `CHANGES`, alpaca
  `INCONCLUSIVE`, edgar and fred `CHANGES NOTHING`) and 10 for the strategy, all
  `CHANGES NOTHING`.

Stop both with Ctrl+C.

### A4. The deployed app

```bash
databricks bundle run console -t dev
```

Open the app's URL and its **Results** tab. ✅ The same rows as A3. That tab reads as the app's
service principal, so this checks the schema grant; `PERMISSION_DENIED` on `results` means the
grant did not apply: send Claude the deploy output.

## Part B: prod

Deploy only while no agent queue pass runs (`make status TARGET=prod` lists none as running).

```bash
make deploy-prod
databricks bundle run smoke -t prod
databricks bundle run console -t prod
```

✅ The smoke run ends `OK: pitlake_prod has all layers, landing volume, a writable control schema
and the published results views`. On the **Results** tab, for backtest `a4d4c4e0`:

- Six decisions, at 2026-06-01, 2026-08-27 and 2026-10-08 for each decider. The strategy's
  causing counts are 84, 84 and 105; the agent's 1 at 2026-06-01 (`bar:MSFT/2026-05-29`) and
  0 at the other two. Every `null_changes_nothing` is true.
- Three strategy sets of 84, 84 and 105 bars, each confirmed and coherent; agent sets appear as
  the agent's sufficiency finishes.
- Sources: the strategy's 14 sources, 8 of them `CHANGES NOTHING`; the agent's rows appear when
  its data value prints `OK`.

Send Claude anything that differs.

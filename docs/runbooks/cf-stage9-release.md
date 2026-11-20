# Counterfactual Stage 9 runbook: release

The `release` job exports, per instant of a recorded backtest, a world bundle: every item knowable
at 06:00 UTC that day for the agent's five symbols, with the Gold rows derived from them. It reads
each bundle back with the released engine, `pitlake-counterfactual`, and fails unless the engine
rebuilds it, hashes its items to PITLake's information set and withholds a sample of items exactly
as PITLake does ([ADR 0031](../adr/0031-release.md)). The engine is a wheel with no dependency: a
third party installs it, withholds items from a bundle and runs their own agent in each world.
Stage 9 is complete when someone outside PITLake does that with an agent of their own and their
result passes `make releasecheck`.

Stages 0 to 2 must be complete first. Part B uses Stage 3's prod decisions for comparison. Follow
the parts in order. If you see something other than what a step says, stop and keep the full
output. Every time is UTC.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes and `Deployment complete!`. The deploy builds three wheels, `pitlake`,
`pitlake-collector` and `pitlake-counterfactual`.

### A2. Export a dev bundle

```bash
databricks bundle run release -t dev
```

Under 15 minutes. ✅ `TERMINATED SUCCESS` with `RELEASED: world-2025-01-15-` and the first eight
characters of the latest dev backtest, `at 2025-01-15 06:00 UTC`, its set, items, rows and size,
and `engine 0.1.0 verifies it and withholds 9 items as PITLake does`, or fewer when the set holds
fewer than three items of a kind (dev may hold no filing in reach), then its path; last `OK: 1 bundles
released from pitlake_dev; download them with` and a `databricks fs cp` line. Run that line from
`data/release/`:

```bash
mkdir -p data/release
cd data/release
```

and then the printed `databricks fs cp dbfs:/Volumes/pitlake_dev/control/releases/...` command,
and `cd ../..`. Send Claude the output.

### A3. The engine outside the checkout

A new virtual environment in a temporary folder, with nothing from the repository but the wheel and
the bundle:

```bash
make engine
OUT=$(mktemp -d)
python3 -m venv $OUT/cf
$OUT/cf/bin/pip install dist/engine/pitlake_counterfactual-0.1.0-py3-none-any.whl
cp data/release/world-2025-01-15-*.json.gz $OUT/
cd $OUT
cf/bin/pitlake-counterfactual inspect world-2025-01-15-*.json.gz --entity AAPL --limit 3
```

✅ The bundle line with the same items and rows as A2, `VERIFIED: the items hash to the recorded
set` with A2's set, and AAPL's last three items, the latest `bar:AAPL/2025-01-10` (dev holds AAPL,
SPY and BTCUSDT bars only, AAPL to 2025-01-10). Stay in `$OUT`
for A4.

### A4. Llama 3.3 through the engine

The engine calls the workspace's serving endpoint as any OpenAI-compatible endpoint, with a token
of your login:

```bash
export DATABRICKS_TOKEN=$(databricks auth token --profile pitlake | jq -r .access_token)
cf/bin/pitlake-counterfactual ablate world-2025-01-15-*.json.gz \
  --endpoint https://dbc-cc51bd40-0056.cloud.databricks.com/serving-endpoints \
  --model databricks-meta-llama-3-3-70b-instruct --key-env DATABRICKS_TOKEN \
  --withhold bar:AAPL/2025-01-10 --samples 5 --out result
```

Under 15 minutes. ✅ `VERIFIED`, a `real world:` line with the parts that settled and `stable over
2 rounds`, `SAME: null world`, one line for `without bar:AAPL/2025-01-10` that is `CHANGED`, `SAME`
or `UNCLEAR` with `coherent`, and last `OK`. ❌ `FAILED: no part settles in the real world` is a
finding about the agent at 5 samples, not a bug: run again with `--samples 10`. Send Claude the
output and `result/result.json`.

### A5. The independent check of the result

```bash
cd -
eval $(opam env)
make releasecheck RESULT=$OUT/result
```

✅ For `real` and `world-1`, `IDENTICAL` and `COHERENT`; last `OK: every world in ... is
IDENTICAL and COHERENT`.

### A6. The record

```sql
SELECT name, decision_at, set_hash, items, world_rows, bytes, sha256, checked, verified, problems
FROM pitlake_dev.control.world_bundles
ORDER BY created_at
```

✅ One row, `verified` true, `problems` empty, and `sha256` equal to `shasum -a 256` of the
downloaded file. Send Claude the result.

---

## Part B: prod

Do not deploy to prod while a Stage 7 prod run is in progress. The instants are Stage 3's three
decisions of backtest `a4d4c4e0955046b8a6fb6a171f8b1071`.

### B1. Export the prod bundles

```bash
make deploy-prod
databricks bundle run release -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,at=2026-06-01 2026-08-27 2026-10-08"
```

Under half an hour. ✅ Three `RELEASED` lines, `world-2026-06-01-a4d4c4e0`,
`world-2026-08-27-a4d4c4e0` and `world-2026-10-08-a4d4c4e0`, each with `withholds 9 items as
PITLake does`; last `OK: 3 bundles released from pitlake_prod` and three `databricks fs cp` lines.
Run them from `data/release/` as in A2. Send Claude the output.

### B2. What Stage 3 found at 2026-10-08

```sql
SELECT a.withheld, a.outcomes, a.p_values, a.caused
FROM pitlake_prod.control.attributions a
JOIN pitlake_prod.control.attribution_points p ON p.point_key = a.point_key
WHERE p.backtest_id = 'a4d4c4e0955046b8a6fb6a171f8b1071' AND p.decider = 'agent'
  AND p.decision_at = '2026-10-08 06:00:00' AND a.valid
ORDER BY size(a.caused) DESC
LIMIT 5
```

Send Claude the result. Claude then gives the B3 command with the item Stage 3 found to cause a
part, or, if no row has one, with the latest AAPL bar and filing.

### B3. The engine against Stage 3

Outside the checkout, as in A3, with `world-2026-10-08-a4d4c4e0.json.gz`, Llama 3.3 as in A4,
`--samples 10` and the items Claude gives after B2. Under an hour. ✅ `VERIFIED`, `SAME: null
world` and, for an item Stage 3 found to cause a part, `CHANGED` on the same part; last `OK`.
Then `make releasecheck` on the result, as in A5: `IDENTICAL` and `COHERENT` for every world.
Send Claude the output and `result/result.json`.

### B4. The results

```bash
make results TARGET=prod
```

✅ One line per table, `information_sets` to `world_bundles`, and last `wrote 22 tables of
pitlake_prod to data/results/pitlake_prod`.

---

## Part C: the Stage 9 exit test

### C1. What to send

Before anything leaves the workspace, decide the licence of the engine and whether the bundle's
Alpaca and Binance prices may be shared (ADR 0031). Then send the third party three files: the
wheel `dist/engine/pitlake_counterfactual-0.1.0-py3-none-any.whl`, the bundle
`data/release/world-2026-10-08-a4d4c4e0.json.gz` and `engine/README.md`, which is all they need.

### C2. Their run

They install the wheel, choose what to withhold with `inspect`, and run `ablate` with their own
agent: an endpoint of their own, or `--agent` or `--decider` with their code. They send back the
whole `--out` folder.

### C3. The check

```bash
eval $(opam env)
make releasecheck RESULT=data/release/<their folder>
jq '{passed, config_hash, config: .config.endpoint, bundle: .bundle.id, worlds: [.worlds[] | {name, verdict}]}' data/release/<their folder>/result.json
```

```sql
SELECT name, sha256, verified
FROM pitlake_prod.control.world_bundles
WHERE bundle_id = '<the bundle ID in their result.json>'
```

✅ `OK` from `releasecheck`, `passed` true, one row of `world_bundles` with `verified` true, and a
configuration that names their agent, not a PITLake endpoint. **Stage 9 is complete** when that
holds. Send Claude the three outputs; Claude then records the run in ADR 0031.

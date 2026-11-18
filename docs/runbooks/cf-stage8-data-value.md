# Counterfactual Stage 8 runbook: data value

The `data_value` job groups the items knowable during a recorded backtest by the source that
published them, each feed (`alpaca`, `binance`, `edgar`, `fred`) and each of its symbols or FRED
series, and decides at instants of the backtest in the null world and in a world without every item
of one source at a time. It checks every such world against a full rebuild and records it for the
independent check, then reports per source how many decisions and parts withholding it changed,
next to what Stage 3 found withholding its items one at a time
([ADR 0030](../adr/0030-data-value.md)). A run stops starting worlds after `minutes` and the same
command continues it. Stage 8 is complete when every decision of the prod set is `VALUED`, the
report lists every source, at least one source changes no decision of either decider, and its
worlds pass `make ablationcheck`.

Stage 2 must be complete first. Stage 3's records for the same decisions fill the one-at-a-time
part of the report; without them it is left out. Follow the parts in order. If you see something
other than what a step says, stop and keep the full output. Every time is UTC.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make deploy
```

✅ `make check` passes and `Deployment complete!`.

### A2. The strategy, stopped early

```bash
databricks bundle run data_value -t dev --params deciders=strategy,minutes=0
```

✅ `TERMINATED SUCCESS` with `PARTIAL: strategy at 2025-01-15 06:00 UTC` and either
`no decision in the real world yet` or, when Stage 3's A2 stored one, a line with `0 worlds
decided` and a number `left`; last `PARTIAL: 1 of 1 decisions still have worlds to decide`.

### A3. The same command continues it

```bash
databricks bundle run data_value -t dev --params deciders=strategy
```

Under 20 minutes. ✅ `VALUED: strategy at 2025-01-15 06:00 UTC` with `0 left`, then one line per
world: `null world` first, then `without alpaca`, `without binance`, `without fred` and, if dev
holds a filing in reach, `without edgar`, then the series (`without alpaca/AAPL`,
`without alpaca/SPY`, `without fred/CPIAUCSL` and so on), each `same; ablation coherent`. In dev
every weight at 2025-01-15 is 0 (Stage 3's runbook A2), so no world changes it. Then the `VALUE`
block, one line per source ending `CHANGES NOTHING`, with `one at a time 0 of <n> attributed items
changed a part` where Stage 3 attributed its items, and last `OK: 1 decisions valued in
pitlake_dev; <n> of <n> sources change no decision in the sample`. Send Claude the output.

### A4. The independent check of a world

```sql
SELECT source, ablation_id, withheld, coherent
FROM pitlake_dev.control.source_tests
WHERE decider = 'strategy'
ORDER BY created_at
```

Take the `ablation_id` of `alpaca`:

```bash
eval $(opam env)
make ablationcheck TARGET=dev ABLATION=<ablation ID of alpaca>
```

✅ `IDENTICAL` and `COHERENT`.

### A5. The agent

```bash
databricks bundle run data_value -t dev --params deciders=agent,samples=3,levels=feed
```

Under half an hour. ✅ `VALUED: agent at 2025-01-15 06:00 UTC` with the null world `sampled`, a
world per feed, `sampled` where the agent's tools read the feed and `screened` where they do not,
each with `ablation coherent`; the `VALUE` block; and last `OK`. It reuses the real-world decision
of Stage 3's A4, which used the same `samples`. ❌ `FAILED` with `not reproducible`: no rating
settled in the real world, which is a finding, not a bug. Send Claude the output.

---

## Part B: prod and the Stage 8 exit test

The decision set is Stage 3's: the strategy and the agent at three instants of backtest
`a4d4c4e0955046b8a6fb6a171f8b1071`, 2026-06-01, 2026-08-27 and 2026-10-08. Name the backtest in
every command: the latest prod research run is now the 2024 one of Stage 7.

### B1. What Stage 3 recorded for the set

```sql
SELECT p.decider, p.decision_at, count(*) AS worlds, count_if(size(a.caused) > 0) AS causing
FROM pitlake_prod.control.attribution_points p
JOIN pitlake_prod.control.attributions a ON a.point_key = p.point_key AND a.valid
WHERE p.backtest_id = 'a4d4c4e0955046b8a6fb6a171f8b1071'
GROUP BY ALL
ORDER BY p.decider, p.decision_at
```

Send Claude the result. No row for the strategy: run
[cf-stage3-attribution.md](cf-stage3-attribution.md) B1 first (about half an hour), so the report
can compare each source with its items one at a time. The agent's rows are welcome but not needed.

### B2. The strategy

```bash
make deploy-prod
databricks bundle run data_value -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,deciders=strategy,at=2026-06-01 2026-08-27 2026-10-08"
```

Under an hour. ✅ Three `VALUED` lines with `14 sources; 15 worlds decided, 0 left`, every world
`ablation coherent`; the `VALUE` block with the feeds and series of the symbols the strategy held
`CHANGES`, and `edgar`, `fred` and their series `CHANGES NOTHING` (it reads no fundamental or macro
series, ADR 0026); and last `OK: 3 decisions valued in pitlake_prod`. If the last line is
`PARTIAL`, run the same command again until it is `OK`. Send Claude every run ID and the output.

### B3. Both deciders

```bash
databricks bundle run data_value -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,at=2026-06-01 2026-08-27 2026-10-08"
```

Up to two hours per run, and one or two runs: the strategy's worlds come from B2, and the agent
decides 45 worlds, most of them sampled. Run the same command again while the last line is
`PARTIAL`. ✅ Six `VALUED` lines; the `VALUE` block with, per source, a line for the strategy, one
for the agent and one for `every decider`; and last `OK: 6 decisions valued in pitlake_prod;
<n> of 14 sources change no decision in the sample: ...`. Send Claude every run ID and the output.

### B4. The independent check of every world

```sql
SELECT DISTINCT source, ablation_id, coherent
FROM pitlake_prod.control.source_tests t
JOIN pitlake_prod.control.attribution_points p ON p.point_key = t.point_key
WHERE p.backtest_id = 'a4d4c4e0955046b8a6fb6a171f8b1071' AND t.valid
ORDER BY source
```

A row per source and the null world (`none`), each `coherent`; a source checked in more than one
run has more than one ablation ID, and one of each is enough. For each ablation ID of a source
that `CHANGES NOTHING` in B3, and for `alpaca`:

```bash
eval $(opam env)
make ablationcheck TARGET=prod ABLATION=<ablation ID>
```

✅ `IDENTICAL` and `COHERENT` every time.

### B5. The record

The report:

```sql
SELECT decider, source, level, knowable, changed, decisions, parts_changed, parts,
       round(largest_effect, 4) AS largest, attributed, items_caused, verdict
FROM pitlake_prod.control.source_values
WHERE run_id = (SELECT max_by(run_id, created_at) FROM pitlake_prod.control.source_values)
ORDER BY level, source, decider
```

What the worlds cost:

```sql
SELECT decider, status, count(*) AS worlds, round(sum(seconds) / 60, 1) AS minutes
FROM pitlake_prod.control.source_tests
WHERE valid
GROUP BY decider, status
ORDER BY decider, status
```

Send Claude the results. **Stage 8 is complete** when B2 to B4 hold and B3 names at least one
source. Claude then records the report, the sources that change no decision and what the feed
against series comparison says about each decider in ADR 0030.

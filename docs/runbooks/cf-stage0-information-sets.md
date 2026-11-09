# Counterfactual Stage 0 runbook: information sets

For a recorded backtest, the `counterfactual` job names every daily bar, filing and FRED release
that was knowable at its last decision, rebuilds the backtest's inputs from those items alone, and
fails unless the rebuilt input hash equals the recorded one with no item unaccounted for
([ADR 0022](../adr/0022-information-sets.md)). Stage 0 of the counterfactual information
specification is complete when that holds in prod.

Follow the parts in order. If you see something other than what a step says, stop and keep the
full output. Every time is UTC.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make validate
make deploy
```

✅ `make check` passes, and **Jobs & Pipelines** shows `pitlake-counterfactual-dev`.

### A2. Enumerate the latest backtests

```bash
databricks bundle run counterfactual -t dev
```

✅ `TERMINATED SUCCESS`. The `enumerate` task prints the three Gold versions it read and the set
hash, then one line per backtest starting with `COMPLETE`, each with the items by kind, how many
were read, `IDENTICAL` and `0 unaccounted`, and ends `OK: 24 decisions in pitlake_dev rebuilt from
their information sets alone`.

❌ `no recorded backtest (latest run) to enumerate`: dev has no backtest yet. Run
`databricks bundle run research -t dev --params start=2025-01-02`, then A2 again.

### A3. Pick the drill items

In the SQL editor:

```sql
SELECT s.backtest_id, i.item_id, i.kind, i.status, i.knowable_at
FROM pitlake_dev.control.information_items i
JOIN pitlake_dev.control.information_sets s USING (enumeration_id)
WHERE s.run_id = (SELECT max_by(run_id, created_at) FROM pitlake_dev.control.information_sets)
  AND ((i.kind = 'filing' AND i.status = 'read') OR (i.kind = 'bar' AND i.status = 'unread'))
QUALIFY row_number() OVER (PARTITION BY i.kind ORDER BY i.knowable_at DESC, s.backtest_id) = 1
```

✅ Two rows: the latest filing the backtests read, such as `filing:0000320193-24-000123`, and a bar
none of them read, such as `bar:BTCUSDT/2025-01-01`, with a backtest ID. Dev's period is short
enough that every bar is read; then use an unread filing in A5 instead (change `'bar'` to `'filing'`
in the second condition).

### A4. Drill: leave out a filing that was read

```bash
databricks bundle run counterfactual -t dev --params backtest=<backtest ID>,omit=<filing item ID>
```

✅ The run **fails**. The task prints at least two `unaccounted:` lines naming the filing (its row
`is not in the set`, and a value that `depends on` it), then `INCOMPLETE` with `DIFFERENT`, and the
error says `1 information sets are not complete`.

### A5. Drill: leave out an item nobody read

```bash
databricks bundle run counterfactual -t dev --params backtest=<backtest ID>,omit=<unread item ID>
```

✅ The run **fails** with `INCOMPLETE`, but this time the line says `IDENTICAL`: the hash cannot see
an unread item, and the coverage check does, with `unaccounted:` lines naming the item's rows.

---

## Part B: prod and the Stage 0 exit test

### B1. Deploy and enumerate the Stage 6 backtests

These are the 24 backtests of research run 238695896043288 (2026-10-13, `gold.features` version 4),
whose input hash `pitcheck` rebuilt independently (ADR 0016):

```bash
make deploy-prod
databricks bundle run counterfactual -t prod --params backtest=238695896043288
```

✅ `TERMINATED SUCCESS`: 24 lines starting with `COMPLETE`, each `IDENTICAL` against recorded
`7d0c6270...` with `0 unaccounted`, and `OK: 24 decisions in pitlake_prod rebuilt from their
information sets alone`. Send Claude the **run ID**.

❌ `Gold commit ... has no version of`: the commit predates a Gold table. Send Claude the output.

### B2. Enumerate the latest backtests

```bash
databricks bundle run counterfactual -t prod
```

✅ As B1, for the latest research run. Send Claude the run ID.

### B3. The drills in prod

Run the query of A3 with `pitlake_prod` in place of `pitlake_dev`, then:

```bash
databricks bundle run counterfactual -t prod --params backtest=<backtest ID>,omit=<filing item ID>
databricks bundle run counterfactual -t prod --params backtest=<backtest ID>,omit=<unread item ID>
```

✅ Both runs **fail**, the first with `DIFFERENT`, the second with `IDENTICAL`, each `INCOMPLETE`
with the omitted item in its `unaccounted:` lines. Send Claude the four run IDs.

Claude then checks, read-only, that `control.information_sets` has `complete = true` for every
backtest of B1 and B2, that `rebuilt_input_hash = recorded_input_hash` and `items = read_items +
unread_items` on each, and that the drills' rows have `complete = false`.

**Stage 0 is complete** when B1 and B2 hold and both drills fail as described.

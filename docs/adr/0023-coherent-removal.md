# ADR 0023: Withholding an item rebuilds everything that depended on it

Status: proposed (counterfactual information, Stage 1). Builds on
[ADR 0022](0022-information-sets.md) and [ADR 0016](0016-independent-as-of-check.md).

## Context

Stage 1 of the counterfactual information specification is coherent removal: lineage-driven
recomputation of everything downstream of a withheld item, with its cost measured. It is complete
when an ablated world passes the independent as-of check, contains no trace of the withheld item
in any derived value, and its cost per ablation is known. The specification calls this the
decisive risk. Withholding a filing is not a filter. A world where an item is hidden but still
shapes a derived value is not a counterfactual; it is a bug.

Stage 0 named the items (bars, filings, FRED releases), gave every row of `gold.features` an owner,
and gave every value a lineage.

## Decisions

**An item is withheld where it entered Gold.** A bar is withheld from `gold.bars_1d`, a filing from
`gold.fundamentals` (every fact it reported), and a release from the vintages of its series, which
are rebuilt from the FRED level rows of `gold.features` because Silver keeps old versions for only
seven days (ADR 0022). The ablated world is `gold.features` as Gold's own functions
(`price_features`, `fundamental_features`, `macro_features`) would have built it from what is left.

**Lineage decides what is recomputed.** Rows owned by a withheld item are removed. A row whose
lineage includes a withheld bar is removed too, and recomputed with `gold.price_features` over the
bars left, starting 21 sessions before the first such row, which is as far back as any window
reaches. Only those exact rows are added back; a 20-day volatility whose window no longer has 21
bars stays gone. A filing or a release has no derived value outside its own rows in this feature
set: a fundamental feature comes from one filing, and `cpi_yoy` compares months within one vintage.
Removing them is removing their rows, and `as_of` then falls back to the latest earlier filing or
vintage, which is what was genuinely known without them.

**The lineage of a price is its whole window.** The first Spark test of a removal failed: after a
bar was withheld, the closes of the next 20 sessions no longer matched the full rebuild. Gold gives
every price feature the `knowable_at` of the latest of the 21 bars ending at its day, so a close's
knowable time depends on the 20 bars before it as well as its own. Stage 0 had given `close` one
bar and `return_1d` two. All four price features now have the 21-bar lineage, or as many bars as a
series has at its start.

**A full rebuild is the reference.** Each ablation is also built from scratch: the three Gold
functions run over the bars, facts and vintages minus the withheld items, and the result must
equal the incrementally ablated world row for row, values and knowable times exactly. A derived
value that still carries the item, such as a volatility computed with a withheld bar, differs from
the rebuild. Before any item is withheld, the null world is rebuilt the same way and must equal the
recorded Gold version exactly, and must give the recorded input hash; otherwise the rebuild could
not be trusted as a reference. An ablated world is coherent when:

1. it equals the full rebuild;
2. no row belongs to a withheld item;
3. the Stage 0 accounting holds within it: every row knowable at the decision has an owner among
   the items left, and every value the backtest reads there has its whole lineage among them.

**An independent program checks the result.** `make ablationcheck TARGET=<target> ABLATION=<id>`
exports the ablated world (the base version with the ablation's changes applied), the bars left,
and the withheld items, and `tools/pitcheck` checks it with its own code:

- the as-of hash of the ablated world equals the one the job recorded;
- no row is owned by a withheld item, and no withheld bar is among the bars;
- every `close` and `volatility_20d` equals what its own implementation computes from the bars left
  (natural log returns, sample standard deviation of 20 of them, the latest `knowable_at` of the 21
  bars), values within a relative 1e-9 and knowable times exactly, and none is missing.

Fundamental and macro rows are values copied from a single item, so for them the ownership check
is the trace check. The OCaml price code shares nothing with Spark's, and the two agree on the
constructed world to that tolerance.

**Worlds are stored as changes.** `control.ablations` has a row per ablated world: the withheld
items, the set hashes before and after, the rows removed and recomputed, the backtest's input hash
in that world, the comparison with the full rebuild, the accounting, and the seconds spent removing,
reading the inputs and rebuilding. `control.ablation_changes` holds the removed and added rows; the
recorded Gold version with those changes is the world. Thousands of worlds cost their changes, not
36,000 rows each.

**The `ablate` task runs after `enumerate`.** It reuses the enumeration's world. The null world runs
first. `withhold` takes item IDs separated by spaces and withholds them together, as one world.
`sample` withholds, one at a time, that many read items of each kind, spread evenly over time.
`mode=hide` removes only the items' own rows without recomputing anything, as a drill: a withheld
bar must then fail against the full rebuild and in `pitcheck`. The task fails if any world is not
coherent.

**Cost is measured, not estimated.** Removal (planning and recomputation) is what an experiment
pays per world; reading the backtest's inputs through `as_of` is what a decision in that world
costs; the full rebuild is verification and would not run in every experiment.

## Consequences

- An item's passages in `gold.filing_chunks` are not removed yet. No backtest reads text; when the
  agent becomes a decision (Stage 2), the filing item will own its passages and a removal will have
  to drop them from the exact search.
- The incremental and full paths share Gold's functions, so a mistake in those functions would be
  repeated by both. The OCaml price check and `as_of` check are the independent ones, as in
  ADR 0016.
- A change to `gold.price_features`' window must change `lineage` in `pitlake.information`; the full
  rebuild comparison now catches the mismatch.
- Tests: `tests/test_ablation.py` (the plan, the window, hiding, applying, sampling, the records),
  `tests/spark/test_ablation.py` (the null world equals Gold; withholding a bar removes its close,
  the 14 closes after it and 5 volatilities, and recomputes 14 closes and 4 volatilities, equal to
  the full rebuild; hiding it differs by 5 and 4 rows; a filing and a release together), and the
  OCaml tests of owners, prices and traces.

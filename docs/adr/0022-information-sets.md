# ADR 0022: A decision's information set, item by item

Status: proposed (counterfactual information, Stage 0). Builds on
[ADR 0008](0008-gold-and-point-in-time-access.md), [ADR 0009](0009-research-and-reproducible-backtests.md)
and [ADR 0016](0016-independent-as-of-check.md).

## Context

The counterfactual information specification
([docs/PITLake-counterfactual-information-specification.pdf](../PITLake-counterfactual-information-specification.pdf))
turns PITLake from checking what a decision could know into changing it: withhold some of what was
public at a decision's instant, rebuild the world without it, and see what the decision becomes.
Its Stage 0 comes first because nothing can be withheld before it can be named. For a decision at
an instant, it asks for the complete, finite, addressable set of items that were knowable, and it
is complete when that set reproduces the decision's recorded input hash exactly, with no item
unaccounted for.

Gold already says when each value became knowable, but a value is not an item. `gold.features`
holds rows such as AAPL's `volatility_20d` for a day; what became public was 21 daily bars. The
unit someone could have withheld is the bar, the filing or the FRED release, and every row of
Gold has to be traced back to one.

The only decisions with a recorded input hash today are backtests (ADR 0009). The time-locked
agent (ADR 0021) keeps its runs in memory and records no inputs; that is Stage 2's work.

## Decisions

**An item is a bar, a filing or a release, named by what it is.** Each has an ID that a person
can read and type:

| Kind | ID | Read from | Knowable at |
|---|---|---|---|
| `bar` | `bar:AAPL/2025-03-03` | `gold.bars_1d` | the bar's `knowable_at`, the end of its session |
| `filing` | `filing:0000320193-25-000008` | `gold.fundamentals` | the earliest `knowable_at` of its facts: the acceptance, or the end of the filing date |
| `release` | `release:CPIAUCSL/2025-02-12` | the FRED level rows of `gold.features` (`cpi`, `unrate`, `fedfunds`) of that vintage | the earliest `knowable_at` of those rows: the end of the release day in New York |

A bar is addressed by symbol and day, so a symbol must trade on one venue; the enumeration fails if
it does not. Releases come from Gold rather than `silver.fred_series_vintages` because Gold keeps
its versions for 365 days and Silver only for Delta's default seven, and an old backtest must stay
enumerable. Each item has a digest, the SHA-256 of its content as Gold holds it at the recorded
version (the bar's prices and volume, every fact of the filing, every observation of the
vintage), and a set has a hash over its items' IDs, knowable times and digests. Two enumerations
with the same set hash saw the same information.

**Every row of Gold has exactly one owner, and every value has a lineage.** A row's owner is the
item that made it knowable, read from its `source_table` and `source_ref`: a price feature belongs
to the bar of its day, a fundamental to its filing, a macro value (including `cpi_yoy`) to its
series' vintage. A value's lineage is every item it was computed from. That is its owner, except
for price features, which follow `gold.price_features`: `close` reads one bar, `return_1d` two,
and `volatility_20d` and `dollar_volume_20d` the 21 bars ending at their day, the 20 returns and
the bar before the first. Ownership is what a removal will delete; lineage is what it will have to
recompute (Stage 1).

**A decision's reach bounds the set.** A backtest reads `close`, `volatility_20d`, `net_income_q`
and `cpi_yoy` for its five assets and the `US` entity. Its reach is every item that owns a row of
those: every daily bar of the five symbols, every filing of AAPL, MSFT or NVDA with a quarterly
net income, and every CPI vintage. A 10-K that reports no quarter, the unemployment releases and
Coinbase's bars are knowable but outside this decision's reach, so they are not in its set. The
information set is the items in reach whose knowable time is at or before the decision.

**The decision is the backtest's last one.** A backtest decides once a day at 06:00 UTC, and its
decision on the last day depends, through its price history and positions, on every value it was
given over the period, all of them knowable by then. Its information set is therefore the set at
`period_end` 06:00, and its recorded `input_hash` is the hash that set has to reproduce. The 24
backtests of a research run read the same inputs, so one enumeration serves them all, with a row
for each.

**Complete means three checks pass.**

1. *Reproduction.* The world is rebuilt from the set alone: only the `gold.features` rows in
   reach whose owner is in the set, at the recorded version, written into a new DataFrame. The
   backtest's inputs are read from it through `pitlake.pit.as_of` at every decision of the period,
   exactly as research reads them, and hashed. The hash must equal the recorded one.
2. *Coverage.* Every row in reach that was knowable at the instant has an owner, the owner is in
   the set, and no row became knowable before its owner. A row that fails is unaccounted.
3. *Closure.* Every item in the lineage of every value the backtest was given is in the set. The
   values are read again from the recorded version of the whole table, as research read them, not
   from the rebuilt world: a world that lacks an item falls back to an older one, and its values
   would always be closed.

Each item in the set is then either `read`, in the lineage of something the backtest was given, or
`unread`. The hash alone could not show an item was left out if nothing used it; the coverage check
can, which is what "no item unaccounted for" asks for.

**The `counterfactual` job enumerates and records.** `pitlake-counterfactual-<target>` has one
task, `enumerate`, and no schedule. With `backtest` set to a backtest or research run ID it
enumerates those; without it, the latest research run. It reads the recorded `gold.features`
version and, from `control.gold_commits`, the `gold.bars_1d` and `gold.fundamentals` versions the
same Gold commit wrote. It appends one row per item to `control.information_items` and one row per
backtest to `control.information_sets` (the counts by kind, read and unread, the items knowable only
later, the set hash, both input hashes, the unaccounted count with a sample, and `complete`), then
fails if any backtest is not complete. Later stages add their tasks to this job.

**A drill proves each check can fail.** `omit` takes one item ID and leaves it out of the set on
purpose. Leaving out a filing the backtest read changes the rebuilt hash and leaves its rows and
values unaccounted. Leaving out a bar nobody read keeps the hash identical, and the coverage check
still finds its row. Both must fail the job. This is not yet a counterfactual: it hides an item
without recomputing what depended on it, which the specification calls a bug, and Stage 1 replaces
it with coherent removal.

## Consequences

- Items are as fine as Gold keeps them: a daily bar, not its trades or minute bars, and a filing's
  facts. Passages of `gold.filing_chunks` carry the accession, so they will belong to the filing
  item when a decision reads text.
- The rebuild uses the same `as_of` as research. What makes the result more than code agreeing with
  itself is that the recorded hash came from the whole table weeks earlier and was rebuilt by an
  independent implementation (ADR 0016), and that bars and filings are read from different tables
  than the rows they own.
- Lineage of the price windows copies `gold.price_features`. If the window changes there,
  `BARS_BEHIND` in `pitlake.information` must change with it; the hash check would not notice.
- An enumeration reads about 40,000 Gold rows into the driver and runs `as_of` twice, over the
  recorded table and over the rebuilt world, so a set costs about twice a backtest's inputs.
- Tests: `tests/test_information.py` builds a small world in Python and checks ownership, lineage,
  reach, digests, the set at an instant and each kind of unaccounted item. `tests/spark/test_information.py`
  runs on the leakage suite's constructed world through Spark and `as_of`: the set rebuilds the
  recorded hash with nothing unaccounted, leaving out the 10-Q that was read changes it, and leaving
  out an unread bar keeps it but is caught.

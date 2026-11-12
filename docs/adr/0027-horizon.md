# ADR 0027: The horizon keeps only recent information and finds where a decision stops holding

Status: proposed (counterfactual information, Stage 5). Builds on
[ADR 0023](0023-coherent-removal.md), [ADR 0025](0025-attribution.md) and
[ADR 0026](0026-sufficiency.md).

## Context

Stage 5 of the counterfactual information specification is the horizon: truncate the information
set by recency. It is complete when a degradation curve is measured per task type, with the
truncation point at which decisions stop being reproducible.

Sufficiency (ADR 0026) finds a minimal set of items that still gives a decision, but that set can
mix old and new items, and the search can take hundreds of worlds. The horizon asks a narrower
question that a reader can act on directly: how far back does a decider need to see? Every
truncated world is a world Stage 1 can build: keep what became public in the last few days before
the instant and withhold everything older, recomputing every price feature whose window lost bars.

## Decisions

**A horizon keeps the items that became public within that many days before the instant.** For a
decision at instant `t` (ADR 0025: a decider at an instant of a recorded backtest, read at the
Gold versions it recorded) and a horizon of `h` whole days, the world keeps every item knowable at
`t` whose `knowable_at` is after `t - h` days, and withholds every other item knowable by the
backtest's last decision, as ADR 0026's worlds do. `h = 0` keeps nothing. The untruncated world,
`full`, keeps everything knowable at `t`. A horizon is counted in calendar days rather than
sessions, so that bars, filings and releases are cut by the same rule.

**The curve is decided over a ladder of horizons, longest first.** The ladder is `horizons`, by
default 365, 180, 120, 90, 60, 45, 30, 21, 14, 7, 3, 1 and 0 days. The untruncated world is decided
first and must give the same decision as the real one, since nothing it withholds was knowable at
the instant; if it does not, the decision fails, and nothing shorter is decided. Then every horizon
of the ladder is decided, including those below the first that fails, because the whole curve is
the measurement.

**Same means what it means in ADR 0026.** For the strategy, all five weights are exactly the
same; for the agent, the world passes as ADR 0025's null world must: every settled rating settles
to the same rating, no p-value is below 0.05, and enough samples are valid. Each world also
records two degrees of sameness: `same`, the share of the decision's parts that stay the same, and
`agreement`, the mean over its parts of the share of samples that give the real decision's value
of that part, with an invalid sample counted as not agreeing. For the strategy both are the share
of weights that stay the same. The curve of a decision is these three values at every horizon.

**The truncation point is found to the day.** Between the shortest horizon of the ladder above
which every horizon gives the same decision and the first one that does not, the gap is halved
until it is one day: `holds_to` is the shortest horizon that still holds, `breaks_at` the day
below it. When every horizon of the ladder holds, `holds_to` is its shortest and there is no
`breaks_at`. When the first horizon of the ladder fails already, the halving starts from one day
longer than the age of the oldest item, which keeps everything. Halving assumes the decision does
not flip back and forth inside the gap; on the ladder itself, a horizon below `breaks_at` that
gives the same decision again is reported as a recovery, which is Stage 6's question rather than
this one's.

**Each part has its own horizon too.** For every part of the decision, the curve gives the
shortest horizon above which that part stays the same. For the strategy that says how far back
each symbol's weight needs data; a symbol with a weight of zero stays zero with no data at all.

**A world is decided once per set of items it keeps.** Horizons longer than the history keep the
same items as `full`; worlds are recorded in `control.horizon_tests` under the hash of the items
they keep, as in ADR 0026, so such a horizon reuses the world already decided, and a later run
reuses every world an earlier one decided. The strategy is evaluated in every world. The agent is
screened against the tool calls of the real world (ADR 0025): a world that returns the same text
for every one of them gives the same conversations, so it holds without sampling, with the
agreement of the real world. Any other world is sampled `samples` times, all of them, so that its
agreement is measured rather than stopped early as ADR 0026's search does. A world where the
endpoint failed for a sample is not kept, and a later run decides it again; an answer that does
not parse is a valid measurement of a decision that was lost.

**Every world is checked against a full rebuild, and the two either side of the truncation point
are recorded as ablations.** With `verify=all` (the default) every world decided is compared with
a full rebuild of Gold without the items it withholds, at about 3 seconds each; `verify=boundary`
skips that. The world at `holds_to` and the world at `breaks_at` are recorded in
`control.ablations` and `control.ablation_changes` with Stage 1's coherence check, as ADR 0026's
confirmation is, so `make ablationcheck` can check both independently.

**The `horizon` job.** `pitlake-horizon-<target>` has one task, `truncate`, with the parameters
`backtest`, `at`, `deciders`, `samples`, `min_agreement`, `horizons`, `verify` and `minutes`. It
stops starting worlds after `minutes` (100 by default, under the two-hour timeout). The real world
of a decision is shared with Stages 3 and 4 through `control.attribution_points` under the same
key. Each decision prints `MEASURED` with its truncation point, one line per horizon (items kept,
same or `DIFFERS`, `same` and `agreement`, and the ablation of each boundary), and each part's own
horizon; `PARTIAL` while worlds are left, and the same command continues it; or `FAILED` when the
real decision is not reproducible, the untruncated world does not decide the same, a world differs
from its rebuild, or a boundary ablation is not coherent, and the job then fails. After the
decisions it prints, per decider, the curve over every decision measured: at each horizon of the
ladder, how many decisions reproduce and the mean `same` and `agreement`. It writes
`control.horizon_tests` (one row per world decided), `control.horizon_curves` (one row per
decision and horizon, with the ladder, `holds_to`, `breaks_at` and the boundary ablations; a
decision with rows for the same ladder is complete), and, for every agent sample,
`control.decision_samples` with the world's test ID as `point_id`.

## Consequences

- The strategy of the prod backtest (lookback 20, no inflation brake, no profit filter) needs the
  last 21 sessions of each symbol it holds (ADR 0026), so its truncation point should be the age
  of the oldest of those bars: 29 to 32 days at an instant where it holds an equity, since 21
  NYSE sessions span four weeks and more with weekends and holidays, and 21 days where it holds
  only BTCUSDT. In
  the Spark test world, where every symbol has a bar every day, it holds down to 21 days and breaks
  at 20, and the weight of a symbol it does not hold stays zero at every horizon.
- The agent's horizon is a property of its tools as well as its reasoning: `symbol_snapshot`
  reports moves over up to 60 sessions and the latest quarterly results, and `price_history` up to
  180 days, so its texts change at most horizons below half a year. In the Spark test a scripted
  agent that rates AAPL by the sign of its last daily move holds down to 2 days and breaks at 1,
  where only one AAPL bar is left.
- The cost of a curve is the untruncated world and the ladder, 14 worlds by default, and about
  log2 of the gap it halves, 2 to 8 more, against hundreds for a sufficiency search. A strategy world costs one
  plan, one small Spark query and a 3 second rebuild; an agent world that changes its texts costs
  `samples` samples.
- A truncation point says how much recent information is enough, not which of it: a decision that
  holds to 29 days may need only a few of the items in them, which is what ADR 0026 measures.
- Curves are comparable across decisions only for the same ladder; `control.horizon_curves` keeps
  the ladder on every row, and a run with another ladder measures the decision again, reusing
  every world already decided.
- The boundary ablations withhold every item outside the horizon, so `withheld` in
  `control.ablations` lists thousands of items for these worlds, as for ADR 0026's.
- The base decision and the ablation record are now shared helpers,
  `pitlake.jobs.attribute.new_base` and `pitlake.jobs.suffice.ablation_of`, used by the
  attribution, sufficiency and horizon jobs.
- Tests: `tests/test_horizon.py` (what a horizon keeps, the halving to the day, recoveries,
  decisions that hold everywhere, nowhere or break at the first horizon, what a world measures for
  each decider, the records and reading them back with the last run winning, a world decided once
  per set of items, when a decision is measured or fails, the summary per decider, the job) and
  `tests/spark/test_horizon.py` (the strategy holds to 21 days and breaks at 20, with both
  boundaries coherent ablations and every world equal to its rebuild; a later run reads the curve
  or continues it and a late one stops; the scripted agent holds to 2 days and breaks at 1, with
  its sampled worlds flipping AAPL; a world where the endpoint failed leaves the decision partial
  and the next run decides only that world).

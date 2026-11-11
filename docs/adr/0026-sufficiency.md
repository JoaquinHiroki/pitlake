# ADR 0026: Sufficiency searches for a minimal set of items that gives the same decision

Status: proposed (counterfactual information, Stage 4). Builds on
[ADR 0023](0023-coherent-removal.md), [ADR 0024](0024-decision-harness.md) and
[ADR 0025](0025-attribution.md).

## Context

Stage 4 of the counterfactual information specification is sufficiency: search for the smallest
subset of the information set that reproduces a decision. It is complete when minimal sets are
found for a sample of decisions and each is confirmed by reconstructing the decision from that set
alone. Attribution (ADR 0025) asks what a decision loses when one item is withheld; sufficiency
asks the reverse, what it needs when nearly everything is.

Two things make it harder than attribution. The space is every subset of the information set, 2 to
the power 2,637 at the last instant of the Stage 2 backtest, so no search can be exhaustive. And
withholding nearly everything is the hard case for coherent removal: every bar left has a window
that lost bars, so its price features must be recomputed from the bars that remain.

## Decisions

**A sufficient set is judged by the world built from it alone.** The decisions are those of
ADR 0025: a decider (`strategy`, `agent`) at an instant (`at`) of a recorded backtest, read at the
Gold versions the backtest recorded, with the same key, so a decision Stage 3 already recorded in
`control.attribution_points` is reused and one recorded here serves Stage 3. A set of items
knowable at the instant is tried by withholding every other item knowable by the backtest's last
decision, including those that became public after the instant, through Stage 1's coherent
removal. The set gives the same decision when, for the strategy, all five weights are exactly the
same, and for the agent, the world passes as ADR 0025's null world must: every settled rating
settles to the same rating, no p-value is below 0.05, and enough samples are valid.

**Minimal means that no single item can go.** A set is minimal when it gives the same decision and
withholding any one more item does not. That is 1-minimality: a smaller set with different items
may exist, and the set found is not unique. When the strategy needs 21 closes of a symbol, any 21
with the same last window would do, and the one found is the one the search reached first.

**The search drops chunks, coarse first.** The first world tried withholds everything. If the
decision still holds, the set is empty. Otherwise the items are split into kinds (bars, filings,
releases), each kind into entities, and each entity into halves by when they became public, older
first, and each chunk is tried, breadth first: a chunk whose removal keeps the decision is dropped,
and one whose removal does not is split again until it is one item. Then every item left is tried
alone, oldest first, and dropped if the decision holds without it, until a whole pass drops
nothing. The last pass is what makes the set minimal: an item that was needed when it was tried
can stop being needed once other items are gone, because withholding is not monotone when windows
are recomputed. Every world tried is recorded in `control.sufficiency_tests` under the hash of the
items it kept, and the search is deterministic, so a later run replays the recorded worlds without
building them and continues from the first one missing.

**Worlds are built from the lineage index, and their windows are kept.** A world is
`pitlake.ablation.Lineage.plan` of the items withheld (ADR 0025), with the price rows of each
symbol recomputed by one `recompute_each` query. The rows recomputed for a symbol depend only on
its bars in the window and the rows wanted, so they are kept under that key and reused by every
later world that needs the same ones; a world that only drops a chunk of BTCUSDT bars recomputes
BTCUSDT alone.

**The agent is screened against the last world it was sampled in.** As in ADR 0025, the agent sees
a world only through its tools. A world that returns the same text for every call made in the last
world that held, the real one first, gives the same conversations and passes without sampling;
it is recorded as `screened`. A world that changes a text is sampled `samples` times (10 by
default), two at a time, and sampling stops as soon as some settled rating can no longer reach
`min_agreement` or too many samples are invalid: such a world fails whatever the rest would say.
A world that holds after sampling becomes the one the next worlds are screened against. A world
with too few valid samples is not kept, and a later run tries it again.

**The set is confirmed from itself alone, three ways.** When the search ends, the world built from
the set alone is decided again: the strategy twice, which must agree with each other and with the
real weights; the agent in two rounds of `samples`, which must be reproducible as ADR 0024 defines
it, settle every rating that settled in the real world to the same rating, and pass as a null
world. That world must equal a full rebuild of Gold without the items withheld. And it is recorded
as an ablation in `control.ablations` and `control.ablation_changes`, restricted to the features
the backtest reads, with Stage 1's coherence check (rebuild, nothing owned left, every value
accounted for), so `make ablationcheck` can check it independently.

**`verify` decides which other worlds are rebuilt.** With `verify=final` (the default) only the
confirmed world is checked against a full rebuild; with `verify=all` every world tried is, at
about 3 seconds each. A world that differs fails the decision.

**The `sufficiency` job.** `pitlake-sufficiency-<target>` has one task, `suffice`, with the
parameters `backtest`, `at`, `deciders`, `samples`, `min_agreement`, `verify` and `minutes`. It
stops starting worlds after `minutes` (100 by default, under the two-hour timeout). Each decision
prints `SUFFICIENT` with the set, how many worlds were tried and sampled, and the confirmation;
`PARTIAL` while the search or the confirmation is not done, and the same command continues it; or
`FAILED` when the real decision is not reproducible, the set alone does not decide the same, or a
world differs from its rebuild or is not a coherent ablation, and the job then fails. It writes
`control.sufficiency_tests` (one row per world tried: the items kept and their hash, the chunk
dropped, the phase, whether the decision held, how it was decided, the samples, whether sampling
stopped early, and what the world cost), `control.sufficient_sets` (one row per decision: the
set, its size by kind, the worlds tried and sampled, the confirmation and its ablation), and, for
every agent sample, `control.decision_samples` with the world's test ID, or the confirmation's ID,
as `point_id`.

## Consequences

- The strategy of the prod backtest (lookback 20, no inflation brake, no profit filter) reads only
  closes and 20-day volatility, so its sets should hold only bars: the last 21 sessions of each
  symbol it holds, which is the volatility window and the lookback together, and nothing for a
  symbol whose weight is zero, since a symbol without bars has weight zero. In the Spark test world
  the search finds exactly that: 42 bars of 52 items, from 97 worlds.
- A sufficient set states what a decision needs, not what its decider read. The agent can read
  hundreds of closes through `price_history` and still give the same ratings from a few of them;
  in the Spark test, a scripted agent that rates AAPL by the sign of its last daily move needs
  exactly its last two bars.
- The cost of a search grows with the size of the set it finds, not the size of the information
  set: dropping a chunk of irrelevant items is one world, but isolating k needed items costs
  about two worlds each, and the last pass tries each again once later drops changed the world.
  For the strategy a world costs one plan and at most one small Spark query; for the agent, a
  world that changes its texts costs up to `samples` samples, fewer when it fails.
- Screening is exact only for the calls the agent made in the worlds it was sampled in, as in
  ADR 0025; the confirmation samples the agent afresh in the world of the set alone.
- The p-value of a sampled world is not corrected for the many worlds of a search. A world that
  passes by chance drops an item the decision needs; the confirmation is what shows that the set
  still gives the same decision.
- The confirmation's ablation withholds every item outside the set, so `withheld` in
  `control.ablations` lists thousands of items for these worlds.
- `pitlake.jobs.attribute.Writer` now takes the tables it writes, so both jobs use it.
- Tests: `tests/test_sufficiency.py` (how chunks split, the search finds what a decision needs and
  an empty set in one world, the set found always holds and no single item can go for random
  decisions, when sampling stops, reusing worlds and stopping late, the screen moves only on a
  sampled world, the records and reading them back, when a decision is sufficient, the job) and
  `tests/spark/test_sufficiency.py` (the strategy needs the last window of each symbol it holds,
  confirmed, rebuilt and a coherent ablation; a later run replays every world without building
  one and a late run stops; a scripted agent needs only the two bars its rating reads, with BTCUSDT
  screened and failing worlds stopped early).

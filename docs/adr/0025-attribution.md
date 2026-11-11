# ADR 0025: Attribution withholds each knowable item and measures what the decision becomes

Status: proposed (counterfactual information, Stage 3). Builds on
[ADR 0022](0022-information-sets.md), [ADR 0023](0023-coherent-removal.md) and
[ADR 0024](0024-decision-harness.md).

## Context

Stage 3 of the counterfactual information specification is attribution: leave-one-out ablation
across a bounded set of decisions. Withhold one item at a time from what was knowable at a
decision, decide again, and the items whose removal changes the decision are the ones that caused
it. It is complete when attribution scores exist for every decision in the set, and a null
ablation that withholds nothing measurably changes nothing.

The three earlier stages give the pieces: the items knowable at an instant (ADR 0022), a coherent
world without any of them (ADR 0023), and two deciders sampled over any world, with the parts of a
decision that settle and the parts that do not (ADR 0024). What they leave open is cost. Stage 1
measured about 3 seconds to build a world without a bar and read the backtest's inputs from it;
the last instant of the Stage 2 backtest has 2,637 knowable items. An agent sample costs about 10
seconds, so sampling the agent ten times in every one of those worlds, two at a time, would take
about a day and a half per decision.

## Decisions

**A decision is a decider at an instant of a recorded backtest.** The bounded set is the deciders
(`strategy`, `agent`) times the instants named in `at`, each a day at 06:00 UTC within the
backtest's period, read at the Gold versions the backtest recorded. Its candidates are every item
in its information set at that instant, read or not.

**The parts of a decision are what is attributed.** For the strategy they are its five target
weights; the `result` hash of Stage 2 is the whole path of returns up to the instant, which every
earlier bar changes, so it says nothing about the decision. For the agent they are its five
ratings, and only those that settled in the real world (ADR 0024): a rating that flips on its own
cannot be attributed.

**The real world is decided once and kept.** The strategy is decided twice and must agree; the
agent is sampled in two rounds of `samples` (2 × 10 by default) and must be reproducible as
Stage 2 defines it. For each settled part the decision keeps how many valid samples support it,
and for the agent every tool call its samples made. All of it is stored in
`control.attribution_points` under a key of the decider, its configuration hash, the instant, the
Gold versions, `samples` and `min_agreement`, so a later run continues from the same base.

**The strategy is attributed exhaustively.** A strategy decision costs milliseconds, so every
candidate is withheld, one at a time, and the strategy decides again in that world. A weight is
`changed` if it differs at all, and its effect is the absolute change in weight. An item causes the
parts it changed. There is no sampling and no p-value: the strategy is deterministic.

**The agent is screened, then sampled where its inputs change.** The agent sees a world only
through its tools. If withholding an item leaves the text of every tool call the agent made in the
real world identical, then every conversation it had there is the same conversation in the
ablated world, and its decisions come from the same distribution: that item cannot change the
decision, and it is recorded as `screened` with no effect. Only items that change what one of
those calls returns are sampled, and of those the job tries every filing and release and, per
symbol, `bars` bars (3 by default) spread over time and always including the latest. Bars cost the
same as filings to sample and are many: a bar inside the last 61 sessions shifts a price move the
snapshot shows, so roughly 300 bars can change the agent's inputs at an instant. The screen assumes
the agent stays on the calls it made in the real world; in the first prod run (566052270466908) all
ten samples made the same calls.

**An agent part has an outcome, an effect and a p-value.** An ablated world is sampled `samples`
times (10 by default). Per settled part: the outcome is `same` if the part settles to the same
rating, `flipped` if it settles to another, and `unsettled` if no rating reaches `min_agreement`;
the effect is the share of samples supporting the real rating in the real world minus that share
in the ablated world; and the p-value is the two-sided Fisher exact test of those two counts. An
item causes a part when the outcome is not `same` and the p-value is below 0.05. A world where
fewer than `min_agreement` of the samples are valid is recorded as `invalid` and sampled again in
the next run.

**The null world is run like any other, and must change nothing.** It goes through the same
removal path with nothing withheld. The agent is always sampled in it, never screened: it is what
measures how far a world with identical information moves the agent's support. It passes when
every settled part is `same`, no p-value is below 0.05, and its samples are valid. The strategy's
null passes when every weight is exactly the same. A decision whose null fails is reported as
failed.

**Every world that changes a decision is checked against a full rebuild.** As in Stage 1, Gold is
rebuilt from the bars, facts and vintages without the item and must equal the ablated world row
for row. With `verify=changed` (the default) this is done for the null world and every world with
a part that is not `same`, which are the worlds a claim rests on; `verify=all` checks every world,
and `verify=none` none. A world that differs from its rebuild fails the decision.

**Worlds are built from one lineage index and one recomputation.** `pitlake.ablation.Lineage`
indexes every row of the world by the items that own it or lie in its window, so a plan no longer
scans the world; `plan` is now that index. `recompute_each` recomputes the price rows of every
withheld bar in one Spark query: each bar's window is given a symbol of its own (`AAPL~17`), so
Gold's `price_features`, partitioned by symbol, computes each window apart from the others, and the
rows are mapped back. A Spark test shows it gives the rows that recomputing each bar alone gives.

**Runs continue where they stopped.** The job reads the points and the valid worlds already
recorded under each key, decides only what is missing, writes every ten minutes, and stops
starting worlds after `minutes` (100 by default, under the two-hour timeout). A run that stops
early prints `PARTIAL`; the same command continues it. Each decision prints `ATTRIBUTED` when its
null world and every candidate it has to attribute are done, `PARTIAL` while some are left, and
`FAILED` when the real decision is not reproducible, the null world changed it, or a world differs
from its rebuild; the job then fails.

**The `attribution` job.** `pitlake-attribution-<target>` has one task, `attribute`, with the
parameters `backtest`, `at`, `deciders`, `samples`, `min_agreement`, `bars`, `verify` and
`minutes`. It writes `control.attribution_points` (one row per decision), `control.attributions`
(one row per decision and world: what was withheld, whether the decider's inputs changed, the
outcome, effect and p-value per part, the parts it caused, the rows removed and recomputed, and the
rebuild check) and, for every agent sample, `control.decision_samples`, with the world's ID as
`point_id`.

## Consequences

- The strategy's result for a decision is a complete leave-one-out over its information set. The
  agent's is complete over filings and releases and over the items its tools never show, and a
  sample over bars; the job prints how many items changed the agent's inputs and how many of those
  it attributed.
- The p-values are per part and per world, not corrected for the many worlds of a decision. With
  20 real and 10 ablated samples, a part must lose most of its support for p to fall below 0.05;
  the null world measures how far identical information moves it.
- An agent part already unsettled in the real world has no attribution. In the first Stage 2 runs
  BTCUSDT and NVDA were unsettled at 2026-10-08.
- Screening is exact only for the calls the agent made in the real world. An agent whose calls
  vary between samples is screened against all of them together, which screens less.
- Cost per decision (estimated from Stage 1 and 2): the strategy about 0.2 seconds per world plus
  about 3 seconds per changed world checked; the agent a few minutes of screening, then about 50
  seconds per sampled world with 10 samples two at a time.
- `control.decision_samples` now also holds attribution samples; their `point_id` is a world ID
  from `control.attributions`, not a point of `control.decision_points`.
- The harness job's `samples` default is 10, as this ADR and ADR 0024 say; it was 5.
- Tests: `tests/test_attribution.py` (the Fisher test, the strategy's and the agent's effects, the
  null world, screening on the calls made, the choice of bars, the records and reading them back,
  when a decision is attributed, the job) and `tests/spark/test_attribution.py` (one recomputation
  equals one per bar and the full rebuild; the strategy attributes every item with an unchanged
  null world and continues from its records with nothing left; a late run stops; a scripted agent
  is screened on the bars it never reads, sampled on the ones it does, and flips when the last
  AAPL bar is withheld).

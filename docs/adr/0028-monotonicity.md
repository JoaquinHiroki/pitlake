# ADR 0028: Monotonicity supplies more information at random and tests whether decisions get worse

Status: proposed (counterfactual information, Stage 6). Builds on
[ADR 0023](0023-coherent-removal.md), [ADR 0024](0024-decision-harness.md) and
[ADR 0027](0027-horizon.md).

## Context

Stage 6 of the counterfactual information specification is monotonicity: supply information in
increasing quantity and measure decision quality. It is complete when non-monotonic regions are
identified, or their absence established, at stated significance.

Stages 3 to 5 asked whether a decision stays the same when information is withheld. Stage 6 asks
whether it gets better, which needs two things the earlier stages did not: a measure of quality,
and a way to add information that is not also a change in which information is added. The horizon
(ADR 0027) already adds information in one order, newest first, so its curve mixes "more" with
"older". This stage adds it in random orders, so that quantity is the only thing that changes along
a curve.

## Decisions

**Quantity is a share of the information set, added in a random order.** For a decision at
instant `t` (ADR 0025: a decider at an instant of a recorded backtest, read at the Gold versions it
recorded), the items knowable at `t` are put in a random order, seeded by the hash of the
information set and the draw number, so the same draw is the same order in every run. The world at
share `f` keeps the first `round(f * n)` items of that order and withholds every other item knowable
by the backtest's last decision, as ADR 0026's worlds do. The worlds of one draw are nested: a
larger share keeps everything a smaller one does. The shares are `levels`, by default 0, 0.1, ...,
0.9 and 1, and each decision is measured over `draws` orders, by default 5. Share 0 keeps nothing
and share 1 keeps everything knowable at `t`, so those two worlds are the same in every draw and are
decided once.

**Quality is the active return of the decision's tilt over the days after the instant.** The
outcome of a decision is the return of each symbol from its last close public at `t` to its last
close public at `t + days`, by default 30 days, the month the agent is asked to rate. Each decision
is read as a tilt per symbol: the strategy's weight, and for the agent +1/5 for overweight, 0 for
neutral and -1/5 for underweight. Its quality is the sum over symbols of the tilt times the
symbol's return minus the mean return of the symbols, so an equal tilt, all neutral, or no answer
at all scores 0, and only the cross-section of the decision counts. A world's quality is the mean
over its samples, with a sample that gave no valid answer scored 0. The outcome is read from the
latest `gold.features` close rows, not from the recorded Gold versions, since the window usually
ends after the backtest was recorded; closes that were public at `t` are the same in both. A
decision whose window is not over yet, because no close of some symbol became public after
`t + days`, fails rather than being scored on part of its window. Symbols the lake has no close for
at all are left out of the score. This is a scoring rule for comparing decisions with each other,
not a claim that any of them would be profitable (the specification excludes that).

**Every pair of shares is tested, with a sign-flip test and Holm's correction.** A unit is one
draw of one decision: its qualities at every share. Units are pooled per decider over the decisions
of the run, so the prod set gives 3 decisions times 5 draws, 15 units. For every pair of shares
`a < b`, the difference in quality from `a` to `b` is taken per unit, and a one-sided sign-flip
test gives the p-value that quality falls (and, separately, that it rises): exact over every sign
pattern for up to 12 units, otherwise 10,000 random patterns with a fixed seed. The p-values of the
falls are adjusted with Holm's method over all pairs, as are those of the rises; with 11 shares
that is 55 pairs. The stated significance is 0.05 after adjustment.

**A non-monotonic region is where a larger share is significantly worse.** The intervals `[a, b]`
of the pairs that fall are merged where they overlap, and each merged interval is reported as a
non-monotonic region. A decider with no such pair is reported as monotonic at 0.05, with the
strongest fall the test could not reject. Its curve also gives where information stops helping:
the smallest share from which no pair of larger shares rises significantly. The test assumes only
that, under the null, a unit's difference is as likely to be positive as negative; the draws of a
decision share its outcome, so the inference is about random subsets of this bounded set of
decisions, not about decisions in general.

**Every world is decided in full and checked against a full rebuild.** The strategy is evaluated
in every world. The agent is sampled `samples` times in every world, with no screening against the
real world's tool calls, since a random half of the items changes almost every text the agent
reads, and with no early stop, since quality is a mean over the samples. A world where the endpoint
failed for a sample is not kept, and a later run decides it again. With `verify=all` (the default)
every world is compared with a full rebuild of Gold without the items it withholds; `verify=none`
skips that. The world of the first draw at the share nearest one half is recorded in
`control.ablations` and `control.ablation_changes` with Stage 1's coherence check, so
`make ablationcheck` can check one world per decision independently.

**A world is decided once per set of items it keeps.** Worlds are recorded in
`control.monotonicity_tests` under the hash of the items they keep, as in ADRs 0026 and 0027, with
every decision sampled in them, so a later run reuses every world an earlier one decided and can
score it against a different outcome window.

**The `monotonicity` job.** `pitlake-monotonicity-<target>` has one task, `supply`, with the
parameters `backtest`, `at`, `deciders`, `samples`, `levels`, `draws`, `days`, `verify` and
`minutes`. It stops starting worlds after `minutes` (100 by default, under the two-hour timeout).
Each decision prints `MEASURED` with its outcome and, per share, the items kept and the mean,
lowest and highest quality over the draws, and the ablation checked; `PARTIAL` while worlds are
left, and the same command continues it; or `FAILED` when its outcome is not known yet, a world
differs from its rebuild, or the ablation is not coherent, and the job then fails. Once every
decision of a decider is measured, it prints that decider's verdict: the mean quality per share,
`NON-MONOTONIC` with each region and its strongest falls, or `MONOTONIC`, where information stops
helping, and the change from the smallest share to the largest. It writes
`control.monotonicity_tests` (one row per world decided), `control.monotonicity_curves` (one row per
decision, draw and share, with the returns scored against; a decision with rows for the same
levels, draws and days is complete), `control.monotonicity_pairs` (one row per decider and pair of
shares, with both p-values before and after adjustment), and, for every agent sample,
`control.decision_samples` with the world's test ID as `point_id`.

## Consequences

- The cost of a decision is `(levels - 2) * draws + 2` worlds, 47 by default. A strategy world
  costs one plan, one small Spark query and a 3 second rebuild, so the strategy's prod set takes
  well under an hour. An agent world costs `samples` samples of about 10 seconds each, two at a time,
  so 47 worlds of 10 samples are about 40 minutes per decision and the agent's prod set takes two
  or three runs.
  `draws=3` cuts that to 29 worlds, at the price of 9 units instead of 15.
- The strategy of the prod backtest needs the last 21 sessions of each symbol it holds (ADR 0026),
  so a random half of the items will usually leave it without some of them, and its weights then
  change in steps rather than smoothly. In the Spark test world at 2025-01-15 the strategy has
  fewer than 21 sessions of history, holds nothing and scores 0 at every share.
- In the Spark test world a scripted agent that rates AAPL by the sign of its last daily move
  overweights it when it sees nothing and underweights it when it sees the last session, which
  fell; AAPL then rose 7.14% against BTCUSDT's -3.40% in the 7 days after 2025-01-15, so its
  quality goes from +0.0105 with no information to -0.0105 with all of it. More information made
  that decision worse, which is the shape this stage exists to find.
- Fifteen units and 55 pairs leave little power: after adjustment, a fall must show in nearly every
  unit to be significant. A `MONOTONIC` verdict therefore says that no fall was large and
  consistent enough to detect at 0.05, not that every fall is zero; the pairs table keeps every
  unadjusted p-value for a reader who wants fewer comparisons.
- The 0% world is a decision with no information at all: the strategy holds nothing and the agent
  answers from the prompt alone, which for a model trained after the instant is its parametric
  knowledge. Its quality is the reference the rest of the curve is compared with, and Stage 7
  (decomposition) is where that knowledge is separated from inference.
- The ablation checked per decision withholds about half the items knowable at the instant, so
  `withheld` in `control.ablations` lists thousands of items, as for ADRs 0026 and 0027.
- Tests: `tests/test_monotonicity.py` (shares and their nested orders, the outcome and when it is
  not known, quality for each decider, the exact and sampled sign-flip test, Holm's adjustment, a
  fall found as a region and its absence, where information stops helping, the records and reading
  them back with the last run winning, a world decided once per set of items, when a decision is
  measured or fails, the verdict waiting for every decision, the job) and
  `tests/spark/test_monotonicity.py` (the strategy scored in every world of every draw, each equal
  to its rebuild, with a coherent ablation; a later run reuses every world and a late one stops;
  the scripted agent scored by the tilt of every sample, worse with all the information than with
  none).

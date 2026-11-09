# ADR 0024: A decision harness that samples deciders over any world

Status: proposed (counterfactual information, Stage 2). Builds on
[ADR 0021](0021-time-locked-agent.md), [ADR 0022](0022-information-sets.md) and
[ADR 0023](0023-coherent-removal.md).

## Context

Stage 2 of the counterfactual information specification is the decision harness: deterministic,
seeded, replayable decisions over arbitrary worlds. It is complete when the same world and seed
produce an identical decision across repeated runs, with residual variance measured and bounded.
Attribution (Stage 3) compares a decision in the real world with the same decision in a world
without one item, so a decision that changes on its own would look like an effect.

There are two deciders. The backtest strategy (ADR 0009) is pure Python and deterministic. The
time-locked agent (ADR 0021) is a language model choosing tools; it lives in the console, records
nothing, answers in free text, and reads Gold through SQL rather than from a world.

`databricks-meta-llama-3-3-70b-instruct` rejects a seed: a request with `"seed": 7` is refused with
`json: unknown field "seed"` (checked 2026-11-09). Four identical requests at temperature 0.7
gave the same word, so the endpoint is close to deterministic, but nothing promises it.

## Decisions

**A decision is a constrained answer.** The strategy's decision is its target weights at the
instant and its result hash. The agent's decision is a rating of each of the five symbols as
`overweight`, `neutral` or `underweight` against an equal-weight portfolio for the month after
the instant, given as a JSON object and nothing else. An answer that is not exactly that (another
word, a missing or extra symbol, no JSON) is an invalid sample, recorded with its reason. Free text
cannot be compared between worlds; a fixed format can, and the specification names it as the
response if variance is too high.

**The agent decides from the world, not from Gold.** Its tools read the rows of a world, so the
same agent runs in the real world and in any ablated one:

- `symbol_snapshot(symbol)`: the last close, moves over 1, 5, 20 and 60 sessions, the 20-day
  volatility, and the latest quarterly revenue, net income and diluted EPS, each with when it
  became public;
- `price_history(symbol, days)`: the closes over 5 to 180 days;
- `macro()`: CPI inflation, unemployment and the fed funds rate.

None takes a date. Their reach is close, volatility and those fundamentals for the five symbols,
and the three macro series, which `pitlake.information` enumerates as bars, filings and releases
like the backtest's. The harness agent is a separate loop in `pitlake.harness`; the console's agent
(ADR 0021) is unchanged, since the app does not install the package.

**Worlds are read in Python.** `pitlake.harness.inputs_at` applies the as-of rule to a world's rows
in memory. A Spark test shows it gives exactly what `pitlake.pit.as_of` gives, in the real and in an
ablated world, and in prod the job checks it against the recorded input hash of the backtest (the
`inputs_verified` column): a strategy decision in the real world at the backtest's last instant must
read inputs with that hash. A world with items withheld is built by Stage 1's coherent removal
over the union of both deciders' reach.

**What can be pinned is pinned, and hashed.** A decider's configuration is recorded with its hash:
for the strategy, its parameters and features; for the agent, the endpoint, temperature 0, the
token limit, eight turns at most (five symbols, the macro tool, one spare and the answer, which is
the last turn and is sent without tools), the prompts and the tool definitions. The model cannot
be seeded, so the harness measures what is left instead of assuming it away.

**Each decision point is sampled in rounds.** A point is a decider, an instant, the Gold versions
and the withheld items. The job draws `rounds` × `samples` decisions (2 × 10 by default), two at a
time for the agent. Recorded per sample: the decision, the hash of the inputs the decider read
(the agent's tool calls and their results), the calls, the model calls, the seconds, and the raw
answer.

**A decision settles part by part.** The first prod run (566052270466908, the real world at
2026-10-08) showed why a whole answer cannot be the unit. All ten agent samples read identical
inputs, the same five calls with the same results, and still gave three different answers. AAPL,
MSFT and SPY were rated the same every time; BTCUSDT flipped between overweight and underweight
and NVDA between neutral and overweight, together. Its replay (876273062942985) drew ten more: 20 of
20 again for the three, 12 to 8 and 11 to 9 for the two. The variance is the endpoint's, not the
harness's, and it sits in particular symbols. So each part of a decision (a symbol's rating; the
strategy's weights and result) settles on its own: it takes its most frequent value if at least
`min_agreement` of the valid samples (0.8 by default) agree on it, and is `unsettled` otherwise.
The point's decision is the settled parts with the unsettled ones marked; the strategy's are always
settled. Recorded per point: that decision, the same computed within each round, each part's
share of agreement, the share of samples whose whole answer agrees, and the number of distinct
and invalid answers.

**Residual variance is bounded, part by part.** A point is reproducible when at least one part is
settled, every round reaches the settled parts' values, and at least `min_agreement` of the samples
are valid. The job fails if any point is not. With `replay` set to a harness run, the job decides
those points again, from their recorded backtest, instant and withheld items, and a point
reproduces only if the configuration hash and the decision, settled and unsettled parts alike, are
the same. That is the specification's "same world and seed, identical decision across repeated
runs", across job runs rather than within one. An unsettled part is measured residual variance:
Stage 3 attributes only settled parts, so a rating that flips on its own is never read as an
effect.

**The `harness` job.** `pitlake-harness-<target>` has one task, `decide`, with the parameters
`backtest`, `at`, `withhold`, `deciders`, `samples`, `rounds`, `min_agreement` and `replay`. It
writes `control.decision_points` and `control.decision_samples`.

## Consequences

- A strategy sample costs milliseconds; an agent sample six model calls.
- The agent's rating is a research decision under test, not advice; the console's agent still
  declines to recommend trades.
- A part settled at exactly the threshold can come out unsettled in a replay; the replay then
  reports DIFFERENT, which is the honest result for a rating that close to a coin toss.
- An agent sample costs about 10 seconds; a point of 20 samples, two at a time, about two minutes.
- Tests: `tests/test_harness.py` (the as-of rule, the view and tools at the instant, no date
  parameter, the answer format, the loop with a scripted model, the strategy's determinism,
  settlement by part on the pattern of the first prod run, replay, the records) and `tests/spark/test_harness.py` (the Python as-of equals
  Spark's in a real and an ablated world; the strategy decides identically and verifies its inputs).

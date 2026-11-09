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
and the withheld items. The job draws `rounds` × `samples` decisions (2 × 5 by default), two at a
time for the agent. The point's decision is the most frequent valid one (ties go to the first in
sort order), and each round has its own. Recorded per point: the decision, each round's decision,
the share of samples that agree with it, the share that agree on each symbol (or on the weights
and result of the strategy), the number of distinct and invalid decisions, and the median seconds.
Recorded per sample: the decision, the hash of the inputs the decider read (the agent's tool calls
and their results), the calls, the model calls, the seconds, and the raw answer.

**Residual variance is bounded by a threshold.** A point is reproducible when every round reaches
the same decision and at least `min_agreement` of the samples (0.8 by default) agree with it. The
job fails if any point is not. With `replay` set to a harness run, the job decides those points
again, from their recorded backtest, instant and withheld items, and a point reproduces only if the
configuration hash and the decision are the same. That is the specification's "same world and
seed, identical decision across repeated runs", across job runs rather than within one.

**The `harness` job.** `pitlake-harness-<target>` has one task, `decide`, with the parameters
`backtest`, `at`, `withhold`, `deciders`, `samples`, `rounds`, `min_agreement` and `replay`. It
writes `control.decision_points` and `control.decision_samples`.

## Consequences

- An agent sample costs about 10 seconds and six or seven model calls; a point with 10 samples,
  two at a time, about a minute. A strategy sample costs milliseconds.
- The agent's rating is a research decision under test, not advice; the console's agent still
  declines to recommend trades.
- The per-symbol agreement says how much an ablation has to move a rating before Stage 3 can tell
  it from noise. A symbol whose rating flips among samples of the same world cannot carry an
  attribution on its own.
- Tests: `tests/test_harness.py` (the as-of rule, the view and tools at the instant, no date
  parameter, the answer format, the loop with a scripted model, the strategy's determinism, modes
  and agreement, replay, the records) and `tests/spark/test_harness.py` (the Python as-of equals
  Spark's in a real and an ablated world; the strategy decides identically and verifies its inputs).

# ADR 0024: A decision harness that samples deciders over any world

Status: accepted (counterfactual information, Stage 2). Builds on
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

## Exit test (prod, 2026-11-11)

All runs are on backtest `a4d4c4e0955046b8a6fb6a171f8b1071` (lookback 20, target volatility 10%,
no inflation brake, no profit filter), read at its recorded Gold versions: a world of 35,923 rows
for the strategy and 97,582 for the agent.

The first runs, before parts settled on their own, sampled the agent five times per round. Run
566052270466908 found it not reproducible at 2026-10-08: AAPL, MSFT and SPY agreed in every sample,
BTCUSDT in 0.6 of them and NVDA in 0.5, and the whole answers differed between rounds; the replay
876273062942985 found the same pattern. That is what per-part settlement answers. With it, and
still five samples per round, run 1073910202810318 was reproducible at 2026-10-08 with BTCUSDT and
NVDA unsettled, the world without NVIDIA's 10-Q of 2026-08-26 and AAPL's bar of 2026-06-15 (run
546460924161727) and the instant 2026-06-01 (run 558527752565996) were reproducible for both
deciders, and a replay (run 1073227770134946) found the strategy identical and the agent not
reproducible. That replay is not counted: a point's ID does not include its samples, so the record
does not say whether it replayed the five-sample point of the old format or the new one.

The exit runs use the default of ten samples per round, as Stages 3 and 4 do:

| Run | Decider | Settled | Unsettled (share of the mode) | Whole answers agree | Median per sample |
|---|---|---|---|---|---|
| 507899349790236 | strategy | 5 weights and the result | nothing | 20 of 20 | 0.2 s |
| 507899349790236 | agent | AAPL neutral, MSFT overweight, SPY underweight | BTCUSDT 0.50, NVDA 0.55 | 9 of 20, 4 distinct | 13.8 s, 6 model calls |
| 121710513067715 (replay) | strategy | the same | nothing | 20 of 20 | 0.2 s |
| 121710513067715 (replay) | agent | the same | BTCUSDT 0.60, NVDA 0.60 | 12 of 20, 3 distinct | 11.8 s, 6 model calls |

The strategy is exact: every sample gives the same weights and result, and its inputs from the
pure-Python as-of equal the backtest's recorded hash. The replay of each point is `IDENTICAL`. The
agent's residual variance is confined to two of its five ratings: AAPL, MSFT and SPY settle to the
same rating in both rounds of both runs, each agreed by at least 0.80 of the samples, and BTCUSDT
and NVDA never settle, their most common rating holding 0.50 to 0.60 of the samples in every run
since the first. No sample was invalid. Attribution and sufficiency therefore have three settled
agent ratings at 2026-10-08 to work on, and treat BTCUSDT and NVDA as decisions the agent does not
make there. A point of 20 agent samples, two at a time, took three to four minutes. Stage 2 is
complete.

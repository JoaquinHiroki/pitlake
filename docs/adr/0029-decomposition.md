# ADR 0029: Decomposition separates what the agent remembers from what it infers

Status: proposed (counterfactual information, Stage 7). Builds on
[ADR 0021](0021-time-locked-agent.md), [ADR 0024](0024-decision-harness.md) and
[ADR 0028](0028-monotonicity.md).

## Context

Stage 7 of the counterfactual information specification is decomposition: run the same question
with no tools, from parametric knowledge alone, and through time-locked tools only, at dates either
side of the model's training cutoff, so that apparent skill splits into what was memorised and what
was inferred from the information available. It is complete when apparent skill is decomposed into
memorised and inferred components with confidence intervals.

A model can only have memorised an outcome that happened before its cutoff, so the stage needs
instants on both sides of one. Prod's prices start on 2025-01-01 (ADR 0004), and on 2026-11-13 the
chat endpoints of the workspace were asked, with no tools, for closes they could have remembered:

| Endpoint | Published cutoff | NVDA 2024-03-28 (903.56) | NVDA 2024-06-28 (123.54) | AAPL 2024-07-31 (222.08) | NVDA 2024-10-31 (132.76) |
|---|---|---|---|---|---|
| `databricks-llama-4-maverick` | August 2024 | 903.59 | 123.45 | 224.69 | 118.37 |
| `databricks-meta-llama-3-3-70b-instruct` | December 2023 | 517.55 | 831.82 | 178.88 | 889.90 |
| `databricks-gpt-oss-120b` | June 2024 | refuses | refuses | refuses | refuses |

Maverick remembers closes to the cent until mid-2024 and is 11% off by the end of October 2024.
Llama 3.3, the agent of Stages 2 to 6, misses AAPL's close of 2023-06-30 by a fifth (151.53 for
193.97), so it remembers little in any year. `databricks-qwen3-next-80b-a3b-instruct` gave 875 for
NVDA at the end of 2024 (about 134) and refused later dates as "in the future";
`databricks-qwen35-122b-a10b` reasons for more than 3,000 tokens without answering. No endpoint
remembers any part of 2025, so every instant prod can decide at today is after every cutoff, and
nothing memorised can be separated from anything inferred.

## Decisions

**The decomposition model is Llama 4 Maverick, and prod's prices go back to 2024-01-01.** Its
published cutoff, August 2024, agrees with what it remembers, and it can call the agent's tools when
it must answer by one (below). Its cutoff is taken as 2024-09-01. Binance, Coinbase and Alpaca are
backfilled from 2024-01-01 (FRED and EDGAR already start there or earlier), and a research run from
2024-02-01 records the backtest the instants come from. Backfilled rows carry the `knowable_at` of
their event, so the history is point-in-time like the rest. Llama 3.3 is run the same way as a
control: with its cutoff before the first instant, every instant is after it.

**The same question is asked in three conditions.** All three ask for the rating of ADR 0024
(overweight, neutral or underweight against an equal weight of the five symbols, as JSON):

- *blind*: no tools and no date, from what the model knows. The prompt is the same at every
  instant, so it is sampled once per endpoint and scored against each instant's outcome. It is what
  the model would say about these symbols in any month, its prior;
- *recall*: no tools, and the instant, with the instruction not to use anything published after it.
  It adds what the model can say about that date from memory;
- *tooled*: the agent of ADR 0024 at the instant, on the real world at the backtest's recorded Gold
  versions, through the time-locked tools only. It adds what the tools show.

**Maverick must answer through a tool.** The first dev run, on 2026-11-13, gave a tooled decision
in which all 3 samples were invalid: Maverick made its first tool call in the API's format, then
answered the tool's result in plain text, writing further calls in Llama's own prompt format
(`symbol_snapshot(symbol=SPY)ipython`), making up their results and never giving the JSON. The
same happened whether the assistant turn sent back had empty, null or the returned content, with
the tool's name on its result, with the result as JSON, and with `tool_choice` `auto`. With
`tool_choice` `required` it calls a tool in the API's format every time. So for an endpoint in
`harness.ANSWERS_BY_TOOL`, Maverick alone, the endpoint caller offers a `decide` tool beside the
agent's three, whose arguments are the five ratings, sends `tool_choice` `required`, and turns a
`decide` call into the JSON answer the harness already parses. On the last step, where ADR 0024
offers no tools, it offers `decide` alone. A call without tools in a conversation with no tool
result (blind, recall, the probe) is sent as before. On the test world, three samples looked at all
five snapshots, then two price histories, and decided identically on the last step. The prompts
are those of ADR 0024, and the agent's configuration records `decide` under `answer`, so its
decisions are keyed apart from the invalid ones; every other endpoint answers in text as in Stages
2 to 6, with the same configuration and keys.

Each condition is sampled `samples` times (10 by default) at temperature 0. Quality is ADR 0028's,
the active return of the tilt over the `days` after the instant (30 by default), with a sample that
gave no valid answer scored 0, and a condition's quality at an instant is the mean over its samples.

**The components are differences between conditions, and the cutoff separates memory from
everything else.** At each instant, the prior is the blind quality, the recall gain is recall minus
blind, and the tool gain is tooled minus recall, so the tooled agent's quality, its apparent skill,
is their sum. An instant is before the cutoff when its whole outcome window ends by it and after it
when the instant is at or past it; an instant whose window straddles the cutoff is left out. After
the cutoff the model cannot remember the outcome, so a recall gain there is what naming the date
adds without memory (knowing the regime, say). The two components the stage asks for are:

- *memorised*: the recall gain before the cutoff minus the recall gain after it;
- *inferred*: the tool gain after the cutoff, where tools are the only source of anything about the
  outcome.

The tool gain before the cutoff minus after it is reported too: how much less the tools add when
the model already remembers the answer.

**Intervals are a bootstrap over instants.** Instants are the units, a month apart so that their
outcome windows do not overlap. Each component's 95% interval resamples the instants of each era
with replacement, 10,000 times with a fixed seed, and takes the 2.5% and 97.5% quantiles; a
component needs at least two instants in each era it uses. The inference is about the months of
this lake and these five symbols.

**A recall probe shows what the model remembers.** Before deciding, the job asks the endpoint, with
no tools and no context, for the close of each symbol public at each instant and at each window
end, three times, and keeps the median answer. A time counts as recalled when the median over its
symbols of |ln(answer / close)| is at most `tolerance` (0.05). The probe does not set the cutoff,
which is a parameter decided before any decision is scored, but its count of times recalled before
and after the cutoff is the evidence that the cutoff is where the model's memory ends. The closes
are unadjusted, so an answer adjusted for NVIDIA's split of June 2024 counts as wrong; the median
over five symbols tolerates one.

**The `decomposition` job.** `pitlake-decomposition-<target>` has one task, `decompose`, with the
parameters `endpoint` (default the chat endpoint), `cutoff` (required), `backtest`, `at` (default
the 1st of every month of the backtest's period), `samples`, `days`, `tolerance` and `minutes`. It
prints the probe, `LEFT OUT` for a default instant whose outcome is not known yet or whose window
straddles the cutoff, then per instant `DECOMPOSED` with its era, outcome and the quality of each
condition, `PARTIAL` while conditions are left, or `FAILED` for an instant named in `at` whose
outcome is not known yet. Once every instant is decomposed it prints the components per era with
their intervals, the memorised and inferred components, and `OK`. It stops starting decisions
after `minutes` (100 by default) and the same command continues it: decisions are recorded under a
key of the condition, its configuration, the instant (none for blind), the Gold versions (for
tooled) and the samples, and probes under the endpoint, symbol and session. It writes
`control.recall_probes`, `control.decomposition_tests` (one row per condition and instant decided),
`control.decomposition_scores` (one row per instant), `control.decompositions` (one row per
component and era), and every sample to `control.decision_samples` with the test's ID as
`point_id`.

## Consequences

- No world is ablated: the tooled condition reads the real world, so there is nothing for
  `make ablationcheck` to check. What the tools return is point-in-time by ADR 0021's construction,
  and the Spark test checks that the agent's price history at each instant ends before it.
- Monthly instants from 2024-02-01 give 7 before Maverick's cutoff (2024-02-01 to 2024-08-01) and
  26 after it to 2026-10-01. Seven instants make a wide interval before the cutoff; a backfill to
  2023 would give 19, at about twice the Binance volume.
- The backfill is the expensive part: about a year of Binance trades, which the daily compute quota
  spreads over several days, and the quality gate may meet new days of 2024 when the venues
  disagreed. The decomposition itself is about 300 probe questions, 33 recall and 33 tooled
  decisions of 10 samples, and one blind decision: about an hour for Maverick.
- The recall and tooled prompts both tell the model not to use what it knows after the instant, so
  the memorised component measures what leaks despite that instruction, which is what a backtest of
  such an agent would contain.
- Quality is a scoring rule for comparing conditions, not a claim that any of them would be
  profitable (the specification excludes that).
- Tests: `tests/test_decomposition.py` (the conditions' prompts and configurations, an untooled
  answer, eras, parsing a remembered close and its error, the closes asked about and their median
  error, the components and their intervals with too few instants, arguments and monthly instants,
  blind decided once and every decision reused by its key, a late run and a failed endpoint, every
  instant decomposed and recorded, a later run reusing everything, a named instant with no outcome,
  the records, the job), `tests/test_harness.py` (an endpoint that answers by tool must call one and
  decides by `decide`, offered alone on the last step; only Maverick's configuration records it) and
  `tests/spark/test_decomposition.py` (over Gold built from the test
  world, the tooled agent reads only closes public at each instant and the instants decompose).

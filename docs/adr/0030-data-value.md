# ADR 0030: Data value withholds each source whole and reports which feeds change decisions

Status: proposed (counterfactual information, Stage 8). Builds on
[ADR 0023](0023-coherent-removal.md), [ADR 0024](0024-decision-harness.md) and
[ADR 0025](0025-attribution.md).

## Context

Stage 8 of the counterfactual information specification is data value: attribution aggregated to
the source level, which states which feeds actually alter outcomes. It is complete when there is a
per-source decision-influence report, with at least one source shown to change no decision in the
sample.

Stage 3 withholds one item at a time. Summing its results per source answers a narrower question
than the one a buyer of data asks, because the items of one source stand in for each other. A
FRED release withheld alone leaves the previous vintage in its place, and a daily bar withheld
alone leaves the twenty others in its volatility window, so a source none of whose items changes a
decision alone may still change it when it is gone. The reverse also holds for the agent, which is
sampled on only a few bars per symbol (ADR 0025). A claim that a source changes no decision needs
evidence about the source, not about its items one at a time.

## Decisions

**A source is a feed, or one symbol or series of a feed.** Every item of an information set comes
from one provider: a daily bar from the venue that traded it (`alpaca` for AAPL, MSFT, NVDA and
SPY, `binance` for BTCUSDT, read from `venue` in `gold.bars_1d`), a filing from `edgar`, a FRED
release from `fred`. The feeds are those four. Within a feed, the series are its symbols (bars and
filings) or its FRED series (`fred/CPIAUCSL`, `fred/FEDFUNDS`, `fred/UNRATE`). A series that holds
every item of its feed, such as `binance/BTCUSDT`, is the feed and is not tested twice. `levels`
chooses `feed`, `series` or both (the default).

**Withholding a source removes every item of it, coherently.** The world without a source withholds
every one of its items knowable by the backtest's last decision and keeps everything else, built by
the same lineage as ADR 0026's worlds, so the world is the same for every decision of the backtest
and is built once. Withholding a whole price feed leaves its symbols with no price at all, which
the deciders treat as missing data: the strategy holds nothing of them, and the agent's tools say
no close of it was public at the instant. That is the counterfactual of never having had the feed, not of buying it
elsewhere.

**Decisions are Stage 3's, decided as Stage 3 decides them.** A decision is a decider at an instant
of a recorded backtest, under the key ADR 0025 gives it, so a run reuses the real-world decision
and the attribution records Stage 3 stored for the same decider, instant, Gold versions, `samples`
and `min_agreement`, and records a new one in `control.attribution_points` when there is none. In
each world the strategy is evaluated once; the agent is screened against the tool calls it made in
the real world and sampled `samples` times only when one of them returns something else. The null
world, which withholds nothing, is always decided and the agent is always sampled in it. Per
settled part, the outcome, effect and p-value are those of ADR 0025, and a source causes a part
when the outcome is not `same` and, for the agent, the Fisher p-value is below 0.05.

**Every world is checked twice.** Each world is compared row for row with a full rebuild of Gold
without the source, and recorded in `control.ablations` and `control.ablation_changes` with
Stage 1's coherence check, so `make ablationcheck` can check every one independently. Both checks
run once per world and run, and their result is stored with every decision made in it. A world
that differs from its rebuild or is not coherent fails every decision decided in it, as a null
world that changes a decision or a real decision that is not reproducible does.

**A source changes a decision, changes nothing, or is inconclusive.** Per decider, over the
decisions of the run, a source `CHANGES` decisions when withholding it causes a part of at least
one of them. It `CHANGES NOTHING` when, in every decision, every settled part stays the same with
no p-value below 0.05 and the world is valid, which is the test the null world must pass. Anything
else, such as an agent rating that flips with p above 0.05, is `INCONCLUSIVE`. When a run decides
both deciders, a source is also judged over every decider: it changes nothing only if it changes
nothing for each. The report gives, per source, the items knowable, how many decisions and parts
withholding it changed, the largest effect, and how many of its items Stage 3 attributed and how
many of those caused a part on their own.

**The `data_value` job.** `pitlake-data-value-<target>` has one task, `value`, with the parameters
`backtest`, `at`, `deciders`, `samples`, `min_agreement`, `levels` and `minutes`. It stops starting
worlds after `minutes` (100 by default, under the two-hour timeout) and the same command continues
it: worlds are recorded under the decision's key and the hash of the items withheld. Each decision
prints `VALUED` with a line per world (its items, how it was decided, `same`, `CHANGED` or
`unclear`, the ablation check and the parts caused), `PARTIAL` while worlds are left, or `FAILED`,
and the job then fails. Once every decision is valued it prints the `VALUE` block, one line per
source and decider, and last `OK` with the sources that change no decision in the sample. It
writes `control.source_tests` (one row per decision and world), `control.source_values` (one row
per valuation, decider and source) and, for every agent sample, `control.decision_samples` with the
world's test ID as `point_id`.

## Consequences

- A decision costs one world per source plus the null world: in prod 4 feeds and 10 series, 15
  worlds. The strategy decides in milliseconds, so its cost is the two checks per world, a rebuild
  and an ablation, each a few seconds to a minute. The agent is screened in the worlds whose sources
  its calls never read and sampled in the others, about 50 seconds each with 10 samples.
- The strategy of the prod backtest (lookback 20, no inflation brake, no profit filter) reads only
  closes and volatility (ADR 0026), so `edgar`, `fred` and their series change none of its
  decisions by construction, and the run shows it. Over both deciders, a source changes no
  decision only if the agent, which reads fundamentals and macro series through its tools, does not
  move either.
- `CHANGES NOTHING` for the agent is an absence of a detected change at 0.05 per part, with 20
  real and 10 ablated samples, uncorrected for the many parts and decisions: it says withholding
  the source moved no settled rating far enough to detect, and the null world shows how far
  identical information moves them.
- The feed and its series are reported side by side, so a feed that changes a decision none of its
  series changes alone shows where symbols or series stand in for each other.
- Coinbase is in Gold (BTC-USD) but no decider's universe includes it; it only feeds the
  cross-venue quality gate (ADR 0007). It has no item in any information set, so it is not a
  source here: it changes no decision because no decision can read it, which this stage does not
  need a world to show.
- In the Spark test world the strategy holds AAPL and BTCUSDT on 2025-01-27, so `alpaca` and
  `binance` change it and `fred`, `fred/CPIAUCSL` and `fred/UNRATE` change nothing, each world equal
  to its rebuild and coherent; the world has no filing in reach by then. A scripted agent that reads
  only AAPL's snapshot is sampled without `alpaca` and screened without every other feed.
- Tests: `tests/test_value.py` (an item's feed and series, the sources and the series left out,
  the one-at-a-time tally, the verdicts and their combination over deciders, the arguments, the
  records and reading them back, a decision valued once every world is decided and decided nothing
  again, a late run, a broken world, a moved null world, the values per decider and across them, the
  job) and `tests/spark/test_value.py` (the venues and sources of the test world, the strategy in
  the world without each source with every world rebuilt and coherent and the verdicts, a later run
  that decides nothing again, the scripted agent sampled only where its tools read).

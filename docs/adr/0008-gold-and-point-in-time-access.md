# ADR 0008: Gold, point-in-time access and the leakage audit

Status: accepted (Stage 5). Builds on [ADR 0006](0006-silver-quality-rules.md) and
[ADR 0007](0007-cross-venue-quality-gate.md).

## Context

Spec section 4 makes Gold the research-ready layer: trades compressed into bars, features computed,
and fundamentals and macro data joined on their true publication dates. Section 6 requires that
features be reachable only through a shared function that takes a timestamp and returns the most
recent value knowable at that instant, and that a test suite deliberately construct data whose
knowable_at is later than its event and assert that it never appears in any result. Stage 5 is
complete when the leakage test suite finds zero cases and a company's results are provably invisible
before their filing date.

## Decisions

**Gold runs in the transform job, after the Silver commit.** The job becomes
`plan → transform → gate → commit → gold → commit_gold`. `gold` reads every Silver input at its
committed version (`control.silver_commits`, through `pitlake.snapshots`), so Gold is never built
from Silver that failed the gate. A run whose Silver is not committed skips Gold entirely.

**Four Gold tables.** Each carries `knowable_at`, derived from Silver's and never earlier than any
input row's:

| Table | Grain | knowable_at |
|---|---|---|
| `gold.bars_1m` | venue, symbol, minute | the end of the minute (Binance and Alpaca trades compressed; Coinbase candles as published) |
| `gold.bars_1d` | venue, symbol, day | the end of the day: UTC midnight for crypto (from `bars_1m`), New York midnight for Alpaca's consolidated bars |
| `gold.fundamentals` | symbol, metric, period, filing | the filing's acceptance time, or the end of its filing date when EDGAR has no acceptance time |
| `gold.features` | entity, feature, event, revision | the latest knowable_at of every input the value was computed from |

`bars_1m` is incremental. A Silver file is compressed again only when Silver re-transformed it
(`control.gold_bars_log`, keyed by the file's latest `processed_at`). The other three tables are
small and are rebuilt every run.

**Fundamentals keep every filing.** A metric (revenue, gross profit, operating income, net income,
diluted EPS, operating cash flow, assets, liabilities, equity, cash, shares outstanding) maps to an
ordered list of XBRL concepts, because companies tag the same figure differently: NVIDIA reports
`Revenues`, Apple and Microsoft report `RevenueFromContractWithCustomerExcludingAssessedTax`, and
both used `SalesRevenueNet` before 2018. Within one filing, the first concept listed wins. A fact's
duration decides what it is: no start date is a balance (`assets`), 80–100 days is a quarter
(`revenue_q`), and 350–380 days is a fiscal year (`revenue_fy`). Year-to-date figures (182 and 273
days) are not features. Every filing that reports a period becomes its own row, so a 10-Q/A
restating a quarter adds a row with the amendment's acceptance time, and the original stays (spec
section 6). Q4 is never reported on its own, so `revenue_q` after a 10-K is still the third quarter
until the next 10-Q. Deriving Q4 is left to research.

**One long feature table.** `gold.features` has one row per entity, feature, event and revision:
`event_time` is what the value describes (a trading day, a period end, an observation month), and
`knowable_at` is when it became knowable. `source_ref` points at what it came from: the trading day,
the accession number, or the FRED vintage. Macro features belong to the entity `US` and reach every
entity. The features are deliberately few:

| Group | Features | From |
|---|---|---|
| prices | `close`, `return_1d`, `volatility_20d`, `dollar_volume_20d` | `gold.bars_1d` (BTCUSDT, BTC-USD, SPY, AAPL, MSFT, NVDA) |
| fundamentals | `<metric>_q`, `<metric>_fy`, balances | `gold.fundamentals` |
| macro | `unrate`, `cpi`, `fedfunds`, `cpi_yoy` | every vintage of every observation; `cpi_yoy` compares months within one vintage |

The 20-day features exist only once 20 daily returns do, and their knowable_at is the latest of the
21 bars they read. The table declares a primary key with `knowable_at` as its `TIMESERIES` column,
which makes it a Unity Catalog feature table (spec section 9).

**The only sanctioned read is `pitlake.pit.as_of`.** It takes the features and a spine of
`(entity, as_of)` rows, where `as_of` must be a `TIMESTAMP`, and returns for each entity and feature
the row with the latest `event_time` among those with `knowable_at <= as_of`, choosing the latest
`knowable_at` among revisions of that event. Ordering by event before revision matters: a 10-K
filed in November also repeats last year's figures, and ordering by `knowable_at` alone would
return that old period as if it were the newest. A spine row without an instant returns nothing.
It runs as one sort per entity and feature (a running maximum over `knowable_at`), not a range
join. `read_features` and `features_at` apply it to the committed Gold version.

**Gold is audited before it is written.** `gold` stages every table as Parquet, runs the leakage
audit on the staged tables, and writes Gold only if the audit finds no case. Every check is recorded
in `control.leakage_checks` and every filing in `control.filing_visibility`, whether or not it
passes. `commit_gold` then appends the Gold versions to `control.gold_commits`, which is what
Stage 6 reads. The checks:

| Check | A case is |
|---|---|
| `bar_closed` | a bar knowable before its minute or day ends |
| `feature_after_source` | a feature knowable before what it came from: the filing's acceptance (from `silver.edgar_filings`, independently of the facts), the end of the vintage's release day, or its bar |
| `filing_invisible_before_acceptance` | a feature from a filing that `as_of` returns one microsecond before that filing was accepted. Every filing behind a fundamental feature is probed at that instant and at the acceptance itself |
| `counterfactual` | a difference between `as_of` on the features and `as_of` on features rebuilt from only the Silver rows knowable at that instant. This is checked just before the latest two filings, an older one, the latest two FRED releases and the latest daily bar |

The counterfactual check is the general one. If a computation used anything from after the instant,
deleting that future changes the answer. The other three find the specific mistakes that are
easiest to make.

**The suite runs on every change.** `tests/spark/test_leakage.py` runs in CI with the same audit
functions on a constructed world: two companies, a 10-Q/A restatement, a 10-K without an acceptance
time, a filing whose value (999999) restates a quarter two months late, FRED vintages with a
revision, and 25 days of bars. It asserts that the poisoned value never appears before its
acceptance at any of 192 instants, that the clean pipeline has zero cases in every check including
the counterfactual just before every instant that matters, and that each check catches its leak:
fundamentals dated by period end, bars known when they open, macro dated by observation, and a
price feature that reads the next day's close.

**A drill proves the audit in prod.** `inject_fault=fundamentals_dated_by_period` gives every
fundamental feature its period end as knowable_at, which is the bias of spec section 1. The audit
must fail and Gold must not be written.

## Consequences

- A leak now stops Gold the way a breach stops Silver: the data stays in staging, `gold_commits`
  does not advance, and research keeps reading the last audited version.
- The audit adds a rebuild of the features per counterfactual instant to every run (six by
  default). Features are small; bars are checked by `bar_closed` and are not rebuilt.
- More than half of the EDGAR facts are older than the filings snapshot (about 1,000 filings per
  company), so they carry the end of their filing date instead of an acceptance time. Those
  features become visible up to a day later than they could have, never earlier.
- `bars_1m` includes Alpaca's extended-hours IEX trades; `bars_1d` for equities uses the
  consolidated daily bars instead, so the two do not add up for equities.
- `pitlake.venues.minute_bars`, used by the gate, keeps its own aggregation. Its output is a
  comparison grid, not a Gold table, and the Stage 4 exit test was run on it.

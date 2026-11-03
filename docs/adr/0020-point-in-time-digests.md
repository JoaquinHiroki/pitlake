# ADR 0020: Point-in-time digests and charted answers in the console

Status: proposed (Stage 10). Builds on [ADR 0008](0008-gold-and-point-in-time-access.md),
[ADR 0014](0014-genie-spaces.md) and [ADR 0019](0019-console-web-app.md).

## Context

Retail brokers now put a language model between the investor and the data. Robinhood's Cortex
writes Digests that explain why a stock or a portfolio moved (stocks and crypto in 2025, portfolios
for Gold members from early 2026), builds screeners from a plain-language description and shows
their criteria, results and data source, and its Legend desktop adds custom scans and indicators
described in words. Public's Alpha and Webull's Vega do similar work. All of them describe the
present: a digest is written from whatever was in the news and the data feed when it ran.

PITLake can answer a question none of them answer: what could have been said about a symbol at a
given instant, using only what was public then. Gold already records `knowable_at` for every bar,
fundamental and macro value (ADR 0008), Genie already answers "as known on" questions over it
(ADR 0014), and the chat endpoint the RAG jobs use (ADR 0010) can write prose. The console showed
Genie's answers as tables, and its text as raw Markdown.

## Decisions

**A Digest page writes a symbol's digest as of an instant.** The page takes a symbol (AAPL, MSFT,
NVDA, SPY, BTCUSDT on Binance, BTC-USD on Coinbase) and an instant in UTC; a date alone means
00:00 UTC, as in the Genie spaces. `app/digest.py` holds three parameterised queries over Gold:

- `bars_1d` from 180 days before to 60 days after the instant, each bar flagged
  `knowable_at <= :as_of`;
- the latest value of every feature of the symbol and of `US` with `knowable_at <= :as_of`,
  chosen by the latest `event_time` and then the latest `knowable_at`, which is the rule the
  Markets space is given;
- the last four filings public at the instant and the first one that was not, from
  `gold.fundamentals`, with a filing's period being its latest `period_end` other than the cover
  page's shares date.

The server computes the price moves over 1, 5, 20 and 60 sessions from the known bars only, labels
each value in its unit, and returns the lot. A second call sends those facts to the chat endpoint
(`databricks-meta-llama-3-3-70b-instruct`, temperature 0) with an instruction to use only them,
to say nothing about later events, and neither to predict prices nor to recommend a trade. The
written digest and the list of facts it was given are shown together, and a digest is written
once per symbol and instant while the app runs.

**The facts are checked again before the model sees them.** The queries already filter by
`knowable_at`, and `digest.facts` checks each value and filing a second time and raises
`LeakageFound` if one was public after the instant, so a later change to a query cannot leak
silently. This is the same check-twice approach as the leakage audit of ADR 0008 and the
independent check of ADR 0016, applied to a prompt.

**The chart shows what the digest could not see, on request.** The page draws the closes known at
the instant and marks the instant. "Reveal the sessions after the instant" adds the next 60 days,
faint and shaded from the last known close, so the gap between what was known and what happened is
visible without ever reaching the facts or the prompt. A daily bar is plotted at its session date
but is only public at its `knowable_at` (an Alpaca bar the next morning, UTC), so the shading starts
at the last known close, not at the instant.

**Genie's time series are drawn.** When a Genie result has a date or timestamp column, a numeric
column and at least three dates, the Ask page shows it as a line chart with a Chart and Table
toggle. A string column named like an identity (symbol, venue, entity, feature, metric, workload)
with two or three values splits the lines; more series, or repeated dates without such a column,
fall back to the table. The chart is plain SVG in `LineChart.tsx`, with no charting dependency: 2px
lines, a crosshair and tooltip on pointer and arrow keys, a legend and end labels for two or more
series, and three categorical colours validated for colour-vision deficiency against the
console's light and dark backgrounds. Genie's text is rendered as Markdown (bold, italics, code
and lists) through React elements, never as HTML.

**One more app resource: the chat endpoint.** The app is granted `CAN_QUERY` on
`${var.chat_endpoint}` and reads it as `CHAT_ENDPOINT`, bringing it to 20 resources, the platform
limit. The Gold tables were already granted for the Genie spaces, so no table grant changes. If
`CHAT_ENDPOINT` is unset, the page shows the facts and says it cannot write the digest.

## Consequences

- The app has no free resource slot left. Another grant means removing one first.
- Each written digest is one call to the pay-per-token endpoint; caching per symbol and instant
  keeps repeated demos from repeating it, until the app restarts.
- The digest is only as complete as Gold: no news, analyst notes or social sentiment, which
  Robinhood's Digests use. What it adds is that every sentence can be traced to a value with the
  instant it became public.
- The digest reads the latest Gold tables, as Genie does. Research that must be reproducible still
  goes through `pitlake.pit` on committed versions (ADR 0009).
- Tests cover the queries' point-in-time filters, the grants, the moves, the units, the prompt,
  the leakage check and the endpoints against a fake workspace. The chart and Markdown components
  are checked by the TypeScript build in CI and by eye in the runbook.

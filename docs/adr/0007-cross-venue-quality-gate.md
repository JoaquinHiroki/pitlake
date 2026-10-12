# ADR 0007: Cross-venue comparison and the quality gate

Status: accepted (Stage 4). Extends [ADR 0006](0006-silver-quality-rules.md).

## Context

Spec section 7 has the Transform job compare the two crypto venues and stop the run if the
rejection rate exceeds its threshold. Section 2 says that data missing on one venue and present on
the other is an ingestion fault, not a market event. Stage 4 is complete when an injected
discrepancy stops the job instead of reaching the research layer.

## Decisions

**A gate task between transform and commit.** The transform job becomes
`plan → transform → gate → commit`. `commit` runs only if `gate` succeeds, and the research layer
reads only committed Silver versions. A failed gate therefore keeps every table's committed version
where it was: the data is in Silver, but nothing downstream can see it.

**Three checks, judged per subject and day.**

| Check | Subject | Measured | Failed |
|---|---|---|---|
| `rejection_rate` | `<silver_table>.<rule>` | rows judged that day, in files transformed since the last commit | rows that rule rejected |
| `venue_divergence` | pair (`BTC`) | minutes of the day | minutes whose closes are more than 100 bps apart |
| `venue_missing` | pair | minutes of the day | minutes present on one venue only |

A check fails when `failed > max_rate × measured + tolerance_rows`. The limits live in
`pitlake.gate.THRESHOLDS`, and every run publishes them to `control.quality_thresholds`, the
"quality thresholds" of spec section 4. A more specific subject overrides the default (`*`):

| Check | Subject | max_rate | tolerance_rows |
|---|---|---|---|
| `rejection_rate` | `*` | 1% | 2 |
| `rejection_rate` | `edgar_company_facts.duplicate_key`, `edgar_filings.duplicate_key` | 100% | 0 |
| `venue_divergence` | `*` | 0.1% | 3 |
| `venue_missing` | `*` | 1% | 0 |

EDGAR duplicates are exempt because every snapshot repeats the ones before it (ADR 0004). The
tolerance keeps one bad row in a four-row day from stopping the platform, while a day with all four
rows bad still fails.

**The venue limits come from the data.** Binance BTCUSDT trades are rolled up into one-minute bars:
open and close by trade time and id, high, low, volume, trade count. They are compared with
Coinbase BTC-USD candles on a grid of every minute of the UTC day. Over 2025-01-01 to 2026-10-06,
928,800 minutes:

- Binance was never missing a minute. Coinbase was missing 832 minutes, which are the Stage 3
  sequence gaps.
- The daily mean difference (USDT against USD) ranged from −10.8 to +21.5 bps. The median day's
  worst minute was 8 bps apart, and the 99th-percentile day's worst minute was 31 bps.
- Closes more than 100 bps apart: 13 minutes, all on 2025-10-10, the crash and liquidation cascade
  of that day (worst 454 bps). The next worst minute in the whole period was 53 bps.

With these limits, exactly three days fail: 2025-10-10 on divergence, and the two Coinbase outages
(2025-10-25, 361 minutes missing; 2026-05-08, 402 minutes) on missing minutes. Every other day
passes. The worst is 7 missing minutes, against an allowance of 14.4.

**Accepted breaches are waivers, written by a person.** `control.quality_waivers` holds one row per
check, subject and day, with a reason, who waived it and when. A failed check with a waiver is
recorded as waived and does not stop the gate. Waivers are rows rather than code, so an outage can
be accepted the day it happens. Nothing waives a check by default; the three historical days are
waived by hand in the runbook.

**A day is compared once per pair of Silver files.** `control.cross_venue_log` records the files and
checksums each day was compared from. A day is compared again only if one of its files changes, or
when `reprocess` names a period. A day present on only one venue waits `grace_days` (2) for the
other venue to publish, and then counts in full as missing.

**The comparison is written only when the gate passes.** The gate stages the comparison as Parquet,
judges it, and only then replaces those days in `silver.cross_venue_1m` (one row per minute, with
both venues' bars, the difference in bps and a status) and appends to the log. A failed gate
therefore compares the same days again on the next run and fails again, until the cause is fixed or
waived. Rejection rates work the same way: they are measured over the load-log rows written since
the last Silver commit, so a breach stays in view until a run commits.

**Faults are injected to prove the gate.** `inject_fault=venue_price_divergence` raises Coinbase's
close by 5% from 12:00 to 12:59 UTC on each day of `period_start`..`period_end`, and
`venue_missing_minutes` removes those candles. The gate recompares those days, ignores waivers, and
must fail. If it passes, the task fails anyway, and nothing is written either way. Every check,
injected or not, is recorded in `control.quality_gate_results`.

## Consequences

- A Coinbase outage now stops every Silver commit, for every source, until someone either waives the
  day or finds the data restored. That is the specification's intent: incomplete data must not
  reach the research layer silently.
- Binance's 1-minute bars are computed here and not stored. Stage 5 builds the Gold bars and can
  reuse `pitlake.venues.minute_bars`.
- The first prod gate compares all 645 days in one run, about 2.4 billion trades. The same
  aggregation took 35 s on the SQL warehouse.
- Delta refuses time travel to a version older than its 7-day file retention unless that version is
  the table's latest. Predictive optimization keeps adding `OPTIMIZE` versions, which change no rows,
  so a committed version stops being the latest without its data changing. The first dev run of
  Stage 4 failed this way: Binance was committed at version 12 on 2026-10-05, and two `OPTIMIZE`
  versions followed it. Transform now reads the newest version that holds the same rows as the
  committed one (`pitlake.snapshots`). Only a real write left uncommitted for more than 7 days
  still blocks a read; that means a week without a successful ingest run.

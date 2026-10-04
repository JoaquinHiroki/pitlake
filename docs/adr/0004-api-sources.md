# ADR 0004: Sources served by an API

Status: accepted (Stage 2). Extends [ADR 0002](0002-landing-and-bronze.md), which assumed a source
that publishes finished files with a checksum.

## Context

Binance publishes one archive per day with a SHA-256 next to it. Coinbase, Alpaca, SEC EDGAR and
FRED instead answer HTTP requests: a day of data arrives as several paginated JSON responses, and
nothing states what the bytes should be. Stage 2's exit test is that all five sources load through
the same job with no source-specific handling in the job itself.

## Decisions

**Raw is the response bodies, one per line.** For an API source the collector lands one file per
partition (for Coinbase, one UTC day) in JSON Lines: each line is one response body exactly as
served, in request order. A body that itself contains a line break cannot be framed this way and is
rejected as an invalid response rather than re-encoded, so a landed line is always byte-identical to
what the source sent.

**Validation replaces the published checksum.** Before a partition is accepted, the collector checks
every response: HTTP success, the expected JSON shape, every record inside the window it asked for,
and no record twice. A partition that fails is not landed and is retried on the next run, like a
checksum mismatch. The SHA-256 the collector computes over the landed file goes into the manifest
as before, so everything downstream of landing (verify on load, idempotent `replaceWhere`, the load
log) is unchanged.

**Coinbase lands one-minute candles, not trades.** Measured on 2026-10-04, BTC-USD has about
400,000 trades a day. The public trades endpoint returns at most 1,000 per request and can only be
paged by trade id, so one day costs about 400 requests and a backfill from 2025-01-01 about 250,000.
The candles endpoint returns 300 one-minute candles per request, so one day costs 5 requests. The
cross-venue check (Stage 4) compares Binance trades aggregated to one-minute bars with these
candles. Coinbase omits minutes with no trades; Silver records those gaps rather than filling them.
Trade-level data can be added later as a second dataset, `coinbase.spot_trades`, under this same
contract.

**FRED lands one file per vintage.** A FRED vintage is a series exactly as it was published on one
date. The collector asks FRED for a series' vintage dates, and each date is one partition: the
series' full history requested with `realtime_start = realtime_end =` that date. A past vintage
never changes, so the landed file is final, and its date is when its values became knowable
(spec section 6). The series are the unemployment rate (`UNRATE`), consumer prices (`CPIAUCSL`)
and the effective federal funds rate (`FEDFUNDS`), all monthly. Daily series such as Treasury
yields are left out: they publish a vintage every day, each holding decades of history.

**Formats belong to the dataset, not the job.** A `Dataset` entry declares its `archive` (`zip` or
`none`) and `file_format`: `csv`; `json_rows`, JSON Lines whose lines are arrays of rows (Coinbase);
or `json_records`, JSON Lines whose lines are objects holding an array of records under
`records_path` (FRED's `observations`). `load_bronze` picks a reader from these properties, so a new
source that reuses a format adds an entry and no code. A line that does not parse, or a record of
the wrong shape, is kept in `_corrupt_record` instead of failing the load, as ADR 0002 requires for
CSV.

**API keys live in a Databricks secret scope.** Alpaca and FRED keys go in the `pitlake` scope
(`fred-api-key`, `alpaca-key-id`, `alpaca-secret-key`), and the collector reads them through the
Databricks SDK at run time. On the fallback VM or a laptop, an environment variable such as
`PITLAKE_SECRET_FRED_API_KEY` takes precedence. No key is written to a config file, a job parameter,
a manifest or a log line. FRED takes its key in the query string, and HTTP libraries put the full
URL into their error messages, so the HTTP client redacts credentials from every error it raises
and the log formatter redacts every line it writes.

**One source's outage does not stop the others.** A source whose credentials or listing call fail
is reported as failed, and the remaining feeds still run. The `collect` task still fails, so the
failure email goes out.

## Not decided here

Spec section 7 asks the ingest job to load sources in parallel and to advance the record of what has
been processed only if every source succeeded. Today `collect` handles every feed in one task, and
`load_bronze` advances the load log batch by batch, which is what makes interrupted backfills
resume. How to reconcile the two, and whether to drive a `for_each` task from a source registry
table, will be decided once all five sources exist.

## Consequences

- Landed API files are small (a Coinbase day is about 85 KB, a FRED vintage under 100 KB) and need
  no extraction, so `load_bronze` reads them directly from the landing volume.
- The collector holds no state between requests of one partition. If a run dies halfway through a
  day, nothing is landed for that day, and the next run fetches it again from the start.
- Coinbase reports prices as JSON numbers. Bronze stores the number's text as received; typing them
  is Silver's job, as with Binance.

# ADR 0031: The released engine decides in exported worlds without PITLake

Status: proposed (counterfactual information, Stage 9). Builds on
[ADR 0016](0016-independent-as-of-check.md), [ADR 0022](0022-information-sets.md),
[ADR 0023](0023-coherent-removal.md), [ADR 0024](0024-decision-harness.md) and
[ADR 0025](0025-attribution.md).

## Context

Stage 9 of the counterfactual information specification is the release of the engine, the results
and a write-up. It is complete when a third party runs an ablation against their own agent using
the released engine, without a PITLake checkout.

Everything Stages 0 to 8 run needs PITLake: the information set is enumerated from Gold tables in
Unity Catalog, coherent removal recomputes features with Spark, and the agent calls a Databricks
serving endpoint. Nobody outside the workspace can read the tables, and a checkout alone would not
help, because the data is not in it. A third party needs the world itself, a way to withhold items
from it that is as coherent as Stage 1's, and a way to put their agent where PITLake's agent sits.

## Decisions

**The engine is its own package, `pitlake-counterfactual`, with no dependency.** It lives in
`engine/` as a member of the uv workspace, needs Python 3.11 or later and only the standard
library, and does not import `pitlake`: no Spark, no Databricks SDK, no NumPy. `make engine` builds
its wheel into `dist/engine/`; the bundle builds the same wheel for the release job. Its command,
`pitlake-counterfactual`, has three subcommands: `inspect` verifies a bundle and lists its items,
`world` builds one world and checks it without deciding, and `ablate` decides. Its instructions
for a third party are `engine/README.md`, which is the package's description.

**A world bundle is one instant's information set, not Gold.** The `release` job writes, per
instant of a recorded backtest (06:00 UTC on the day, by default its last decision), one gzipped
JSON file of format `pitlake-world/1`: the raw rows of every item knowable at the instant for the
harness's reach (ADR 0024), that is the daily bars as `gold.bars_1d` holds them, the fundamentals
of each filing as `gold.fundamentals` holds them and the level observations of each FRED release
vintage; the Gold rows in reach knowable by the instant, which are the world; the instant, symbols,
features and the lineage constants (source tables, the 20-session window, the price features, the
series of each macro feature); the information-set hash ADR 0022 computes; and the backtest,
catalog, Gold versions and commit it came from. Nothing knowable after the instant is in it. Its
ID is the SHA-256 of its canonical JSON without the ID, so any change to its content is detected.

**The engine verifies a bundle before it uses one.** It recomputes the content hash and the item
digests and set hash from the raw rows, as ADR 0022 does, and they must equal the recorded ones; it
accounts for every row (owned by an item of the set, knowable by the instant); and it rebuilds
every price feature from the bars, which must equal the Gold rows. The fundamental and macro rows
need no rebuild, because each comes from one item alone (ADR 0023).

**Withholding is Stage 1's removal, checked against a full rebuild.** The engine removes every row
an item owns or reaches through the 21-bar lineage of a price feature, recomputes from the bars
left the price features of rows whose own item is kept, and compares the world with a full rebuild
of every price feature from the kept bars. A world is coherent when no row comes from a withheld
item, every row comes from an item of the set and is knowable by the instant, and the two agree.
The 20-day volatility uses the same one-pass update as Spark's `stddev_samp`, and values are
compared at a relative tolerance of 1e-9, as `pitcheck` compares them (ADR 0023), since a
logarithm may differ in its last bit between libraries.

**The release job proves, per bundle, that the engine withholds as PITLake does.** After writing a
bundle, `pitlake-release-<target>` reads it back with the engine wheel and requires: the bundle
verifies; the engine's set hash equals PITLake's; every answer of the agent's three tools (`macro`,
and `symbol_snapshot` and `price_history` over 180 days for each symbol) is the same text in the
engine's real world as in PITLake's (ADR 0024); and, for `check` items of each kind spread over
the set (3 by default, chosen as Stage 1 samples them), the engine's world without the item equals
PITLake's `decide` world without it, row for row and tool answer for tool answer. It writes the
file to the `releases` volume of the `control` schema, records it in `control.world_bundles` with
its SHA-256, size, the items checked and any problem, and prints `RELEASED` with the command that
downloads it, or `FAILED`, and the job then fails.

**The decision task is Stage 2's.** The engine gives the agent the same system and task prompts,
the same three tools with the same text, the same limits (temperature 0, 700 tokens, 8 steps), the
same parse of the answer, and the `decide` tool for models that answer only through one (ADR
0029). Its agent configuration for an endpoint is byte for byte the one PITLake records, so a run
of the engine with a PITLake endpoint has the configuration hash of Stages 2 to 8, and an agent's
sample has the same input hash for the same calls.

**A third party's agent enters in one of three ways.** An endpoint that speaks the OpenAI chat
completions API, which covers hosted models, local servers such as vLLM or Ollama, and Databricks
serving; a Python function that completes a chat, run in the engine's tool loop; or a Python
function that receives a session, calls the tools itself and returns the ratings, for agents with
a loop or framework of their own. Every tool call goes through the session, which records it.

**The statistics are Stage 3's, and every world is decided.** The real world is decided `rounds`
(2) times `samples` (10); a part settles at `min_agreement` (0.8) and must agree in every round.
The null world, which withholds nothing, and each named world are decided `samples` times, and
each settled part is `same`, `flipped` or `unsettled` with Fisher's exact test against the real
samples (ADR 0025). A world is `CHANGED` when a part moves at p < 0.05, `SAME` when no part moves
and no p-value is below 0.05, `UNCLEAR` otherwise, and `INVALID` when it is not coherent or too few
samples are valid. A run is `OK` when the bundle verifies, a part settles stably in the real world,
the null world is `SAME` and every world is valid. Unlike Stage 3, the engine does not screen: it
cannot assume an agent it does not know depends only on its tool answers, and the exit test is
that the third party's agent runs in the world.

**A third party's result is checked independently.** `ablate --out` writes `result.json`, with
every sample, call and verdict, the bundle ID and the configuration, and a folder per world in the
layout `pitcheck` reads (ADR 0016): the world's rows, the bars left, the items withheld, and, as
the recorded hash, the engine's own as-of hash of the five symbols at the instant. `make
releasecheck RESULT=<folder>` runs `pitcheck` on every world, so a different language and code
base rebuilds the as-of inputs (`IDENTICAL`) and every price feature from the bars left
(`COHERENT`). The bundle ID in the result must be one in `control.world_bundles`.

**The results are exported as files.** `make results TARGET=<target>` writes every table of the
counterfactual program in `control` (ADRs 0022 to 0031) to `data/results/<catalog>/` as CSV, with a
manifest of row counts and SHA-256 digests. The write-up is written from them once the prod runs
of Stages 3 to 8 are recorded in their ADRs.

**CI runs the wheel outside the checkout.** After the other checks, CI builds the engine wheel,
installs it in a new virtual environment in the runner's temporary folder, writes the test world
there and runs an ablation with the scripted agent, so a package that only works inside the
repository fails the build.

**What may leave the workspace is decided by each feed's terms.** The engine is released under
the Apache License 2.0, in `engine/LICENSE` and the wheel's metadata; the rest of the repository
keeps no licence. The data is not covered by it. Every bundle records `terms`: the terms of each
feed it holds and the strictest licence among them, `not for redistribution` (Alpaca), then
`CC-BY-NC-SA-4.0` (Binance Vision), then `public domain` (EDGAR and FRED, citation requested).
`inspect` prints them and `result.json` repeats them, so a result derived from a share-alike
bundle carries its licence. A bundle without `terms` is one to ask about before sharing.

**A shared bundle is the full one without the restricted feed.** `release --without alpaca`
exports, after each full bundle, a second one named `...-without-alpaca`: PITLake's coherent world
with every Alpaca bar withheld, built as Stage 8 builds a source's world (ADR 0030), so stock price
rows vanish and the filings, macro releases and BTCUSDT bars stay. Its set is the set of the items
left, and its `source` records the feeds withheld, the full bundle's name and its set hash. It is
verified as the full one is, against the engine's rebuild and PITLake's removal of sampled items,
and in addition against the engine: the full bundle with the same items withheld must give exactly
its rows. That is the bundle a third party receives; a full bundle never leaves the workspace. Asking
Alpaca for consent was considered and dropped: the exit test needs a third party's agent in an
exported world, which the bundle without Alpaca gives, at the cost of its seeing no stock prices.

## Result (dev, 2026-11-20)

Release run 1026659072377611 exported `world-2025-01-15-0fc8f747`, the last decision of dev
backtest `0fc8f747cd474e90a09bb15b45b1a223`: set `811ba8db…`, 146 items (26 bars of AAPL, SPY and
BTCUSDT, 118 filings, 2 FRED releases), 2,988 rows, 64 kB, SHA-256 `b41842b5…`. The engine verified
it and withheld 8 items (3 BTCUSDT bars, 3 filings from 2009 to 2024, the FEDFUNDS and UNRATE
releases) exactly as PITLake does, rows and tool answers; `control.world_bundles` holds it with
`verified` true and the downloaded file has the recorded SHA-256.

The wheel, installed in a new virtual environment in a temporary folder with nothing else from the
repository, verified the bundle and rebuilt its 2,988 rows. Through the workspace's
OpenAI-compatible serving URL, Llama 3.3 settled all five parts in the real world (AAPL and SPY
underweight, BTCUSDT overweight, MSFT and NVDA neutral) in both rounds of 5 samples, none invalid,
in a median of 21 seconds and 6 model calls, reading only `symbol_snapshot`. The null world was
`SAME`. Without `bar:AAPL/2025-01-10`, AAPL's last session in dev, the world lost one row (dev
holds fewer than 21 sessions, so no volatility to recompute), AAPL flipped in every sample (effect
1.00, p 0.0003) and nothing else moved: `CHANGED`, and `OK`. `make releasecheck` on the result gave
`IDENTICAL` and `COHERENT` for both worlds, with the engine's as-of hashes `9f566e3e…` and
`0af68c9f…` rebuilt by `pitcheck`. The configuration hash of the run is `a82a95e2…`.

## Result (prod, 2026-11-20)

Release run 362756147149455 exported six bundles of backtest `a4d4c4e0955046b8a6fb6a171f8b1071` in
under four minutes, each verified with no problem. A full bundle holds every item of Stage 3's set
at its instant: 2,200, 2,542 and 2,705 items at 2026-06-01, 2026-08-27 and 2026-10-08, one fewer
than the worlds Stage 3 attributed there, which include the null world. Each full bundle is
`not for redistribution` and about 1.2 MB. The bundles without Alpaca hold 792, 890 and 937 items:
every BTCUSDT bar, filing and FRED release, with 1,408, 1,652 and 1,768 Alpaca bars withheld and
about 3% fewer rows. Each is `CC-BY-NC-SA-4.0`, and the engine gave exactly its rows by withholding
those bars from the full bundle. In both kinds the engine withheld nine sampled items (three bars,
three filings, three releases) as PITLake does, tool for tool.

## Consequences

- The decision task now has two implementations, `pitlake.harness` and the engine. Neither imports
  the other, so the released package stays free of Spark. `tests/test_release.py` holds them equal:
  the prompts, tools, configuration, answer parsing, tool texts, input hashes, as-of hash, item
  digests, set hash, Fisher test and settling. A change to the harness that is not made in the
  engine fails those tests, and the release job fails before such a bundle is published.
- The engine withholds bars, filings and FRED releases, the items of ADR 0022. It does not hold
  filing text or the retrieval index (ADR 0010), and it does not run the strategy, whose decisions
  need the whole backtest's history rather than one instant.
- A bundle is a few megabytes compressed. It holds Alpaca's SIP daily bars for SPY, AAPL, MSFT
  and NVDA, BTCUSDT bars aggregated from Binance Vision trades, EDGAR facts and filings, three
  FRED series, and the Gold rows derived from all of them. The data terms, read on 2026-11-20,
  behind the decisions on licences and shared bundles:
  - Alpaca: the customer agreement forbids reproducing or distributing market data without
    Alpaca's written consent, and Alpaca staff on its forum say neither the data nor derivations
    of it may go to third parties. A bundle with Alpaca bars, or price features computed from
    them, cannot be sent without that consent.
  - Binance Vision (Dataset Terms 1.0, 2026-08-26): CC BY-NC-SA 4.0 for the datasets and anything
    derived from them. Academic research and non-commercial open-source evaluation are permitted,
    and a redistributed derivative must credit Binance Vision and keep CC BY-NC-SA 4.0. A bundle
    with BTCUSDT may be shared for research under that licence, never commercially.
  - FRED: UNRATE and CPIAUCSL (Bureau of Labor Statistics) and FEDFUNDS (Board of Governors) are
    "Public Domain: Citation Requested", so a bundle cites the source and FRED. FRED's terms also
    forbid incorporating FRED content into another database without the Bank's permission, which
    a bundle of vintages arguably is; the underlying series are public US government data.
  - EDGAR: public US government records, with no restriction on redistribution.
- The third party is the one outside PITLake who chooses the agent. Their result is evidence for
  the exit test when its worlds pass `make releasecheck`, its bundle ID is in
  `control.world_bundles` and its configuration names an agent PITLake does not run.
- In the Spark test world the engine rebuilds every Gold row of the bundle, matches PITLake's set
  hash, and withholds a bar, a FRED release and a bar in the middle of a volatility window exactly
  as `decide` does. On the engine's own test world `pitcheck` reports `IDENTICAL` and `COHERENT`
  for the real world and for worlds without a bar in the middle of a window and the last bar, and a
  scripted agent that reads AAPL's five-session move is `CHANGED` without AAPL's last bar
  (p 0.012 with 3 samples) and `SAME` without a bar it does not read.
- Tests: `engine/tests/test_engine.py` (verifying, damaged and other-format bundles, removal and
  recomputation, the volatility and its knowable time, as-of, the export, the tools, the task, the
  loop, the `decide` tool, settling, effects, an ablation, a random agent failing the null world,
  the three commands, an OpenAI-compatible endpoint with a retry, loading a function),
  `tests/test_release.py` (the parity above, a bundle holding only what was knowable, the
  strictest terms of its feeds, a bundle without Alpaca that may be shared alike, a bundle without
  terms, a stable encoding, the job's parameters, description and record) and
  `tests/spark/test_release.py` (a bundle released from the Spark test world, its origin, the
  bundle without Alpaca as the full one without its bars, a bundle that is not the full one without the feed caught, a bar in the
  middle of a window, and the command run on the exported bundle).

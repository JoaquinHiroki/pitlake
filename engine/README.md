# pitlake-counterfactual

Withhold information from a point-in-time market world and see what your agent decides without
it.

A **world bundle** is one file exported by PITLake: every daily bar, company filing and US macro
release that was public at one instant (06:00 UTC on a given day) for BTCUSDT, SPY, AAPL, MSFT and
NVDA, with the values derived from them. This package reads a bundle, removes the items you name
and recomputes everything that depended on them (a withheld daily bar changes the volatility of
the twenty sessions after it), checks that the result is coherent, and runs your agent in the real
world and in the world without them. It reports, per symbol, whether the agent's rating changed.

A bundle may leave out a whole feed whose terms forbid sharing it. The bundles PITLake sends
outside have no stock prices (their names end in `-without-alpaca`): they hold the BTCUSDT bars,
the filings and the macro releases, so for SPY, AAPL, MSFT and NVDA the agent sees fundamentals
only. Such a bundle is PITLake's coherent world without that feed, not a damaged one.

It needs Python 3.11 or later and nothing else: no PITLake checkout, no Spark and no Databricks
account.

## Install

```bash
python -m venv cf
cf/bin/pip install pitlake_counterfactual-0.1.0-py3-none-any.whl
cf/bin/pitlake-counterfactual --version
```

## 1. Check the bundle and choose what to withhold

```bash
cf/bin/pitlake-counterfactual inspect world-2026-10-08-a4d4c4e0-without-alpaca.json.gz --kind filing
```

The first lines name the bundle, its instant and its items, then `terms:` with the licence of its
data and one line per feed it holds (see [Licence](#licence)), then `VERIFIED` when the items hash
to the set PITLake recorded and the package rebuilds every derived value from them. Anything else
means the file is damaged or this version cannot read it. Then come the items, one per line, as
IDs:

- `bar:BTCUSDT/2026-10-07`, the daily bar of a symbol on a session;
- `filing:0000320193-26-000013`, an SEC filing with every fundamental it reported;
- `release:UNRATE/2026-09-05`, one vintage of a FRED series (`UNRATE`, `CPIAUCSL`, `FEDFUNDS`).

`--entity BTCUSDT` lists one symbol's items, `--limit 30` the latest thirty.

To see what an agent would read in a world, without deciding:

```bash
cf/bin/pitlake-counterfactual world world-2026-10-08-a4d4c4e0-without-alpaca.json.gz --withhold "bar:BTCUSDT/2026-10-07" --show
```

## 2. The task

At the bundle's instant the agent rates each of the five symbols `overweight`, `neutral` or
`underweight` against an equal-weight portfolio for the month after. It reads the world only
through three tools, which return only what was public at the instant:

- `symbol_snapshot(symbol)`: the latest close, the moves over 1, 5, 20 and 60 sessions, the 20-day
  volatility and the latest quarterly revenue, net income and diluted EPS, as far as the bundle
  holds them;
- `price_history(symbol, days)`: the daily closes over the last 5 to 180 days;
- `macro()`: CPI inflation year over year, the unemployment rate and the fed funds rate.

The answer is a JSON object mapping each symbol to its rating. The package gives the agent the
same prompts, tools and limits (temperature 0, 700 tokens, 8 steps) as PITLake's own experiments,
so results are comparable with them.

## 3. Run an ablation

Each `--withhold` is one world, with every item it names withheld together; repeat it for more
worlds. Your agent is one of three things.

**A chat endpoint that speaks the OpenAI chat completions API** (OpenAI, vLLM, Ollama, LM Studio,
Databricks model serving and others):

```bash
export OPENAI_API_KEY=...
cf/bin/pitlake-counterfactual ablate world-2026-10-08-a4d4c4e0-without-alpaca.json.gz \
  --endpoint http://localhost:11434/v1 --model llama3.1:8b \
  --withhold "filing:0000320193-26-000013" \
  --withhold "bar:BTCUSDT/2026-10-07" \
  --samples 10 --out result
```

`--key-env` names another variable for the key. A model that only answers reliably through a
tool can be given a `decide` tool to answer with: add `--answer-by-tool`.

**A Python function that completes a chat**, `--agent my_agent:complete` (a module importable
from the current folder, or `path/to/file.py:complete`). It receives the OpenAI-style `messages`
and `tools` (`None` on the last step) and returns an assistant message: either
`{"role": "assistant", "content": "<the JSON answer>"}` or one with `tool_calls`, as the OpenAI API
returns it. The package runs the tools and the loop.

**A Python function that decides on its own**, `--decider my_agent:decide`, for agents with their
own loop or framework. It receives a session and returns the ratings, as a dict or JSON text:

```python
def decide(session):
    snapshot = session.call("symbol_snapshot", {"symbol": "BTCUSDT"})
    history = session.call("price_history", {"symbol": "BTCUSDT", "days": 60})
    ...
    return {
        "BTCUSDT": "neutral",
        "SPY": "neutral",
        "AAPL": "overweight",
        "MSFT": "neutral",
        "NVDA": "underweight",
    }
```

`session.at` is the instant, `session.symbols` the five symbols and `session.tools` the tool
definitions. Every call is recorded, and the agent must get its information only through them.

The run decides `--rounds` (2) times `--samples` (10) in the real world, then `--samples` times in
the null world, which withholds nothing, and in each world you named. A part (a symbol's rating)
is settled when at least `--min-agreement` (0.8) of the real samples agree on it, and it is
stable when each round agrees. In a world, a settled part is `same`, `flipped` or `unsettled`,
with Fisher's exact test of the real samples against the world's. It prints one line per world:

- `CHANGED`: withholding the items changed a settled part at p < 0.05;
- `SAME`: every settled part stayed the same, none with p < 0.05;
- `UNCLEAR`: a part moved, not significantly;
- `INVALID`: the world is not coherent or too many samples gave no valid answer.

and last `OK`, or `FAILED` when the bundle does not verify, nothing settles in the real world,
the null world moved the decision, or a world is invalid. The null world shows how much the agent
moves with identical information: if it is not `SAME`, the agent is too random at these settings
for its other worlds to say anything.

## 4. Send the result back

`--out` holds `result.json`, with every sample, tool call and verdict, and `worlds/`, one folder
per world (`real`, `world-1`, ...) with its rows, bars and withheld items. PITLake checks each
folder with an independent implementation of the point-in-time rule and of the recomputation,
written in another language. Send the whole folder.

## Python

```python
from pitlake_counterfactual.world import Bundle
from pitlake_counterfactual.agent import Session

bundle = Bundle.read("world-2026-10-08-a4d4c4e0-without-alpaca.json.gz")
assert not bundle.check()
print(bundle.terms["licence"])
world = bundle.withhold(["bar:BTCUSDT/2026-10-07"])
assert world.coherent
print(Session.of(world).call("symbol_snapshot", {"symbol": "BTCUSDT"}))
```

`pitlake_counterfactual.ablate.ablate(bundle, worlds, decider, config)` runs a whole ablation and
returns the report the command prints.

## Licence

The package is released under the Apache License 2.0; see `LICENSE`.

The data in a bundle is not covered by that licence: each bundle states its own terms, which
`inspect` prints and `result.json` repeats. A bundle without Alpaca's prices is licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/), the licence of the
Binance Vision data in it: use it for non-commercial research only, credit Binance Vision, and
share anything derived from it, such as a result folder, under the same licence. The macro series
are from the U.S. Bureau of Labor Statistics (UNRATE, CPIAUCSL) and the Board of Governors of the
Federal Reserve System (FEDFUNDS), retrieved from FRED, Federal Reserve Bank of St. Louis; the
filings are from the SEC's EDGAR. Cite them when you publish. A bundle whose terms say
`not for redistribution` is for the workspace it was exported from and must not be passed on.

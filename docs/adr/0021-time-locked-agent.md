# ADR 0021: A time-locked agent answers through point-in-time tools

Status: proposed (Stage 10). Builds on [ADR 0020](0020-point-in-time-digests.md).

## Context

The brokers' assistants are becoming agents. Robinhood's Cortex Assistant answers questions about
the customer's portfolio and the market, builds screeners from a description and drafts orders,
and Public's AI Agents act on plain-language instructions. Each plans, calls the platform's own
functions and writes an answer from what they return.

An agent over market data has a specific failure for research: it answers from whatever its tools
and its own training know today, so asked about a past date it mixes in what came later. ADR 0020
fixed that for a fixed set of facts. An agent chooses its own calls, so the guarantee has to hold
for any sequence of them.

`databricks-meta-llama-3-3-70b-instruct`, the endpoint the app already queries, supports tool
calls through its OpenAI-compatible invocations API (checked on 2026-11-03: it called
`symbol_digest(AAPL)` for "Compare Apple and the S&P 500 ETF" in 2.3 seconds).

## Decisions

**The instant is set once, on the server, and no tool takes a date.** `POST /api/agent` takes the
instant and the conversation; `app/agent.py` gives the model five tools whose parameters are only
symbols, a number of days and nothing else (a test checks that no tool has a date parameter):

- `symbol_digest(symbol)`: the facts of ADR 0020's digest;
- `price_history(symbol, days)`: the closes public at the instant over up to 180 days;
- `compare_symbols(symbols)`: two or three symbols' moves and closes indexed to 100;
- `my_portfolio()`: the Portfolio page's holdings, valued as in ADR 0020;
- `macro()`: the US macro values public at the instant.

Each tool is a closure over the instant and reads Gold through the digest's queries, which filter
by `knowable_at`, and through `digest.facts`, which checks every value again. Whatever the model
asks, in whatever order, a tool can only return what was public at the instant. `LeakageFound`
stops the run instead of being shown to the model.

**The model is told the instant and the rules, and its answer is checked for later dates.** The
system prompt says that for the agent it is now the instant, that every number must come from a
tool, and that it may not predict prices or recommend trades. The model's own training does know
later events, so the prompt cannot be the guarantee; the tools are. As a visible check, the
answer's ISO dates after the instant are listed as warnings, except dates the user wrote in the
question, which the agent may repeat while declining.

**The loop is bounded and runs in the background.** The model gets up to six turns; the last is
sent without tools, so it has to answer. A tool error (an unknown tool, a bad argument) goes back
to the model as the tool's result so it can correct itself. The run executes in a thread pool and
the browser polls `GET /api/agent/runs/{id}` every second, as it does for Genie (ADR 0019): the
page shows each tool call as it starts and finishes, what it returned, a chart when the tool
returned a series, then the answer. Runs are kept in memory, the last fifty, and are lost when the
app restarts.

**The conversation lives in the browser.** Each request carries the earlier questions and answers,
not the tool results, and changing the instant starts a new conversation, so a follow-up never
mixes two instants.

## Consequences

- A question costs two to four model calls and its tools' queries: about 13 to 31 seconds on dev
  with a cold cache, less once the five-minute query cache is warm.
- Tool calling is the model's choice. It can pick a less useful tool or answer a vague question
  without one; the trace makes that visible, and the prompt asks for a tool for every number.
- The date check reads ISO dates only. "Next month" or "October" in an answer is not flagged.
- No new resource or grant: the agent uses the chat endpoint and the Gold tables ADR 0020 granted.
- Tests drive the loop with a scripted model: tool results fed back with their call ids, errors
  returned to the model, leaks stopping the run, the final turn without tools, the date check, the
  bounded arguments, and the endpoint running on the locked instant against a fake workspace.

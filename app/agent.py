import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

import digest

MAX_STEPS = 6
MAX_RESULT = 6000
DAYS = (5, 180)
DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

Complete = Callable[[list[dict], list[dict] | None], dict]
Tool = Callable[[dict], tuple[str, dict | None]]

SYMBOL = {"type": "string", "enum": list(digest.SYMBOLS)}


def _tool(name: str, description: str, properties: dict, required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    }


TOOLS = [
    _tool(
        "symbol_digest",
        "Latest close, price moves, reported results with the instant each became public, "
        "and recent filings of one symbol, as known at the locked instant.",
        {"symbol": SYMBOL},
        ["symbol"],
    ),
    _tool(
        "price_history",
        "Daily closes of one symbol over the last given number of days before the locked "
        "instant, with the change, high and low.",
        {"symbol": SYMBOL, "days": {"type": "integer", "minimum": DAYS[0], "maximum": DAYS[1]}},
        ["symbol", "days"],
    ),
    _tool(
        "compare_symbols",
        "Compare two or three symbols at the locked instant: price moves over 1, 5, 20 and 60 "
        "sessions and their closes over the last 90 days indexed to 100.",
        {"symbols": {"type": "array", "items": SYMBOL, "minItems": 2, "maxItems": 3}},
        ["symbols"],
    ),
    _tool(
        "my_portfolio",
        "The user's portfolio at the locked instant: value, changes, weights, each position's "
        "moves and its companies' latest results.",
        {},
        [],
    ),
    _tool(
        "macro",
        "US macro values known at the locked instant: CPI, inflation, unemployment and the "
        "fed funds rate.",
        {},
        [],
    ),
]


@dataclass
class Run:
    id: str
    as_of: str
    status: str = "running"
    steps: list[dict] = field(default_factory=list)
    answer: str | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None

    def view(self) -> dict:
        return asdict(self) | {"done": self.status != "running"}


def system_prompt(as_of: str) -> str:
    instant = as_of[:16].replace("T", " ")
    names = ", ".join(f"{s} ({v.name})" for s, v in digest.SYMBOLS.items())
    return (
        f"You are PITLake's research assistant, working at the instant {instant} UTC. For you "
        f"it is now {instant} UTC: you know nothing that happened or was published after it, "
        "and the tools only return what was public by then. You cover "
        f"{names}, US macro data and the user's portfolio. Get every number from a tool; never "
        "use your own knowledge of markets, prices or results after the instant. Name the date "
        "of each figure you cite. If the user asks what happens after the instant, say you "
        "cannot know it. Do not predict prices and do not recommend buying, selling or "
        "rebalancing. Answer in a short paragraph or a few bullet points."
    )


def later_dates(text: str, as_of: str, asked: str = "") -> list[str]:
    day = as_of[:10]
    named = set(DATE.findall(asked))
    later = sorted({d for d in DATE.findall(text) if d > day and d not in named})
    return [f"The answer mentions {d}, after the instant." for d in later]


def run(state: Run, history: list[dict], complete: Complete, tools: dict[str, Tool]) -> None:
    messages = [{"role": "system", "content": system_prompt(state.as_of)}, *history]
    for step in range(MAX_STEPS):
        reply = complete(messages, TOOLS if step < MAX_STEPS - 1 else None)
        calls = reply.get("tool_calls") or []
        if not calls:
            state.answer = reply.get("content") or ""
            asked = " ".join(m["content"] for m in history if m["role"] == "user")
            state.warnings = later_dates(state.answer, state.as_of, asked)
            state.status = "done"
            return
        content = reply.get("content") or ""
        messages.append({"role": "assistant", "content": content, "tool_calls": calls})
        for call in calls:
            name = call["function"]["name"]
            try:
                arguments = json.loads(call["function"].get("arguments") or "{}")
            except ValueError:
                arguments = {}
            entry = {"tool": name, "arguments": arguments, "done": False}
            state.steps.append(entry)
            try:
                if name not in tools:
                    raise ValueError(f"There is no tool called {name}.")
                text, view = tools[name](arguments)
                entry.update(result=text[:MAX_RESULT], view=view)
            except digest.LeakageFound:
                raise
            except ValueError as error:
                text = f"Error: {error}"
                entry.update(error=str(error))
            entry["done"] = True
            answer = {"role": "tool", "tool_call_id": call["id"], "content": text[:MAX_RESULT]}
            messages.append(answer)
    state.status = "done"


def days(arguments: dict) -> int:
    try:
        found = int(arguments.get("days", 60))
    except (TypeError, ValueError):
        found = 60
    return max(DAYS[0], min(DAYS[1], found))


def symbols(arguments: dict) -> list[str]:
    found = list(dict.fromkeys(arguments.get("symbols") or []))
    if not 2 <= len(found) <= 3:
        raise ValueError("Compare two or three symbols.")
    for name in found:
        digest.symbol(name)
    return found


def history(bars: list[dict], window: int) -> list[tuple[str, float]]:
    known = [(b["session_date"], b["close"]) for b in bars if b["known"] and b["close"] is not None]
    if not known:
        return []
    start = (date.fromisoformat(known[-1][0]) - timedelta(days=window)).isoformat()
    return [(d, c) for d, c in known if d > start]


def history_text(symbol: str, points: list[tuple[str, float]]) -> str:
    if not points:
        return f"No close of {symbol} was public at the instant."
    (first_day, first), (last_day, last) = points[0], points[-1]
    high = max(points, key=lambda p: p[1])
    low = min(points, key=lambda p: p[1])
    closes = "; ".join(f"{d} {c:,.2f}" for d, c in points)
    return (
        f"{symbol} closes public at the instant, {first_day} to {last_day}: "
        f"from {first:,.2f} to {last:,.2f} ({(last / first - 1) * 100:+.2f}%), "
        f"high {high[1]:,.2f} on {high[0]}, low {low[1]:,.2f} on {low[0]}. "
        f"Daily closes: {closes}."
    )


def indexed(series: dict[str, list[tuple[str, float]]]) -> dict[str, list[tuple[str, float]]]:
    starts = [points[0][0] for points in series.values() if points]
    if not starts:
        return {}
    common = max(starts)
    found = {}
    for name, points in series.items():
        kept = [(d, c) for d, c in points if d >= common]
        if kept and kept[0][1]:
            found[name] = [(d, c / kept[0][1] * 100) for d, c in kept]
    return found

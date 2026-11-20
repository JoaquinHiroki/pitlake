import json
import re

SYMBOLS = ("BTCUSDT", "SPY", "AAPL", "MSFT", "NVDA")


def stance(snapshot: str) -> str:
    found = re.search(r"5 sessions ([+-]\d+\.\d+)%", snapshot)
    if not found:
        return "neutral"
    return "overweight" if float(found.group(1)) > 0 else "underweight"


def complete(messages: list[dict], tools: list[dict] | None) -> dict:
    results = [m for m in messages if m["role"] == "tool"]
    if not results and tools:
        call = {
            "id": "c1",
            "type": "function",
            "function": {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})},
        }
        return {"role": "assistant", "content": "", "tool_calls": [call]}
    rating = {s: "neutral" for s in SYMBOLS}
    if results:
        rating["AAPL"] = stance(results[-1]["content"])
    return {"role": "assistant", "content": json.dumps(rating)}


def decide(session) -> dict:
    rating = {s: "neutral" for s in session.symbols}
    rating["AAPL"] = stance(session.call("symbol_snapshot", {"symbol": "AAPL"}))
    return rating

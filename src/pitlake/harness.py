import json
import re
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pitlake.backtest import (
    FEATURES,
    STRATEGY,
    UNIVERSE,
    Params,
    as_utc,
    fingerprint,
    input_fingerprint,
    observations,
    run,
)
from pitlake.gold import GLOBAL_ENTITY
from pitlake.information import Reach

STRATEGY_DECIDER = "strategy"
AGENT_DECIDER = "agent"
DECIDERS = (STRATEGY_DECIDER, AGENT_DECIDER)
STANCES = ("overweight", "neutral", "underweight")
UNSETTLED = "unsettled"
SYMBOLS = tuple(a.entity for a in UNIVERSE)
AGENT_FEATURES = (
    "close",
    "volatility_20d",
    "revenue_q",
    "net_income_q",
    "eps_diluted_q",
    "cpi_yoy",
    "unrate",
    "fedfunds",
)
STRATEGY_REACH = Reach((*SYMBOLS, GLOBAL_ENTITY), FEATURES)
AGENT_REACH = Reach((*SYMBOLS, GLOBAL_ENTITY), AGENT_FEATURES)
HARNESS_REACH = Reach((*SYMBOLS, GLOBAL_ENTITY), tuple(dict.fromkeys((*FEATURES, *AGENT_FEATURES))))
MAX_STEPS = 8
MAX_RESULT = 6000
MAX_TOKENS = 700
TEMPERATURE = 0.0
DAYS = (5, 180)
MOVES = (1, 5, 20, 60)
LABELS = {
    "revenue_q": "Revenue, quarter",
    "net_income_q": "Net income, quarter",
    "eps_diluted_q": "Diluted EPS, quarter",
    "cpi_yoy": "CPI inflation, year over year",
    "unrate": "Unemployment rate",
    "fedfunds": "Fed funds rate",
}
LOWEST = float("-inf")

Complete = Callable[[list[dict], list[dict] | None], dict]
Tool = Callable[[dict], str]


def _rank(row: tuple) -> tuple:
    _, _, event_time, knowable_at, value, source_table, source_ref = row
    return (
        event_time,
        knowable_at,
        source_table or "",
        source_ref,
        LOWEST if value is None else value,
    )


def in_reach(rows: Iterable[tuple], reach: Reach) -> list[tuple]:
    return [r for r in rows if r[0] in reach.entities and r[1] in reach.features]


def inputs_at(
    rows: Iterable[tuple],
    times: Iterable[datetime],
    entities: tuple[str, ...] = SYMBOLS,
    features: tuple[str, ...] = FEATURES,
) -> list[tuple]:
    grouped: dict[tuple[str, str], list[tuple]] = {}
    for row in rows:
        entity, feature = row[0], row[1]
        if feature not in features:
            continue
        targets = entities if entity == GLOBAL_ENTITY else (entity,) if entity in entities else ()
        for target in targets:
            grouped.setdefault((target, feature), []).append(row)
    times = sorted(as_utc(t) for t in times)
    found = []
    for (entity, feature), candidates in sorted(grouped.items()):
        candidates.sort(key=lambda r: r[3])
        best, i = None, 0
        for at in times:
            while i < len(candidates) and candidates[i][3] <= at:
                if best is None or _rank(candidates[i]) > _rank(best):
                    best = candidates[i]
                i += 1
            if best is not None:
                found.append((entity, at, feature, best[4], best[2], best[5], best[6]))
    return found


def inputs_hash(received: Iterable[tuple]) -> str:
    return input_fingerprint(observations(r[:5] for r in received))


def canonical(payload) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class View:
    def __init__(self, rows: Iterable[tuple], at: datetime):
        self.at = as_utc(at)
        self.events: dict[tuple[str, str], dict[datetime, tuple]] = {}
        for row in rows:
            if row[3] > self.at or row[4] is None:
                continue
            events = self.events.setdefault((row[0], row[1]), {})
            current = events.get(row[2])
            if current is None or _rank(row) > _rank(current):
                events[row[2]] = row

    def history(self, entity: str, feature: str) -> list[tuple]:
        return [r for _, r in sorted(self.events.get((entity, feature), {}).items())]

    def latest(self, entity: str, feature: str) -> tuple | None:
        found = self.history(entity, feature)
        return found[-1] if found else None


def stamp(at: datetime) -> str:
    return f"{as_utc(at):%Y-%m-%d %H:%M} UTC"


def symbol(arguments: dict) -> str:
    found = str(arguments.get("symbol", ""))
    if found not in SYMBOLS:
        raise ValueError(f"No symbol {found!r}; choose one of {', '.join(SYMBOLS)}.")
    return found


def days(arguments: dict) -> int:
    try:
        found = int(arguments.get("days", 60))
    except (TypeError, ValueError):
        found = 60
    return max(DAYS[0], min(DAYS[1], found))


def snapshot_text(view: View, name: str) -> str:
    closes = view.history(name, "close")
    if not closes:
        return f"No close of {name} was public at the instant."
    values = [r[4] for r in closes]
    lines = [f"{name}: last close {values[-1]:,.2f} on {closes[-1][2]:%Y-%m-%d}."]
    moves = [
        f"{n} session{'s' if n > 1 else ''} {(values[-1] / values[-1 - n] - 1) * 100:+.2f}%"
        for n in MOVES
        if len(values) > n and values[-1 - n]
    ]
    if moves:
        lines.append(f"Change over {', '.join(moves)}.")
    volatility = view.latest(name, "volatility_20d")
    if volatility:
        lines.append(
            f"20-day volatility of daily returns {volatility[4] * 100:.2f}% "
            f"as of {volatility[2]:%Y-%m-%d}."
        )
    for feature in ("revenue_q", "net_income_q", "eps_diluted_q"):
        row = view.latest(name, feature)
        if row:
            shown = f"{row[4]:,.2f}" if feature == "eps_diluted_q" else f"${row[4] / 1e6:,.0f}M"
            lines.append(
                f"{LABELS[feature]} ending {row[2]:%Y-%m-%d}: {shown} "
                f"(public since {stamp(row[3])})."
            )
    return "\n".join(lines)


def history_text(view: View, name: str, window: int) -> str:
    closes = view.history(name, "close")
    if not closes:
        return f"No close of {name} was public at the instant."
    start = closes[-1][2] - timedelta(days=window)
    points = [(r[2], r[4]) for r in closes if r[2] > start]
    (first_day, first), (last_day, last) = points[0], points[-1]
    high = max(points, key=lambda p: p[1])
    low = min(points, key=lambda p: p[1])
    listed = "; ".join(f"{d:%Y-%m-%d} {c:,.2f}" for d, c in points)
    return (
        f"{name} closes public at the instant, {first_day:%Y-%m-%d} to {last_day:%Y-%m-%d}: "
        f"from {first:,.2f} to {last:,.2f} ({(last / first - 1) * 100:+.2f}%), high "
        f"{high[1]:,.2f} on {high[0]:%Y-%m-%d}, low {low[1]:,.2f} on {low[0]:%Y-%m-%d}. "
        f"Daily closes: {listed}."
    )


def macro_text(view: View) -> str:
    lines = []
    for feature in ("cpi_yoy", "unrate", "fedfunds"):
        row = view.latest(GLOBAL_ENTITY, feature)
        if row:
            shown = f"{row[4] * 100:.2f}%" if feature == "cpi_yoy" else f"{row[4]:.2f}%"
            lines.append(
                f"{LABELS[feature]} for {row[2]:%Y-%m}: {shown} (public since {stamp(row[3])})."
            )
    return "\n".join(lines) or "No US macro value was public at the instant."


def _tool(name: str, description: str, properties: dict, required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    }


SYMBOL = {"type": "string", "enum": list(SYMBOLS)}
TOOLS = [
    _tool(
        "symbol_snapshot",
        "Latest close, price moves over 1, 5, 20 and 60 sessions, 20-day volatility and the "
        "latest quarterly results of one symbol, each as public at the locked instant.",
        {"symbol": SYMBOL},
        ["symbol"],
    ),
    _tool(
        "price_history",
        "Daily closes of one symbol over the given number of days before the locked instant.",
        {"symbol": SYMBOL, "days": {"type": "integer", "minimum": DAYS[0], "maximum": DAYS[1]}},
        ["symbol", "days"],
    ),
    _tool(
        "macro",
        "US CPI inflation, unemployment and the fed funds rate public at the locked instant.",
        {},
        [],
    ),
]


def agent_tools(view: View) -> dict[str, Tool]:
    return {
        "symbol_snapshot": lambda arguments: snapshot_text(view, symbol(arguments)),
        "price_history": lambda arguments: history_text(view, symbol(arguments), days(arguments)),
        "macro": lambda arguments: macro_text(view),
    }


SYSTEM = (
    "You are a research analyst working at the instant {instant} UTC. For you it is now "
    "{instant} UTC: you know nothing published after it, and the tools only return what was "
    "public by then. Get every number from a tool and do not use your own knowledge of markets "
    "after the instant. Rate each of {symbols} as overweight, neutral or underweight against an "
    "equal-weight portfolio of the five for the month after the instant, from the evidence the "
    "tools give. Look at every symbol before you decide."
)
TASK = (
    "Rate each symbol. When you have the evidence, reply with only a JSON object that maps each "
    'of {symbols} to "overweight", "neutral" or "underweight", and nothing else.'
)


def prompts(instant: datetime) -> list[dict]:
    when = f"{as_utc(instant):%Y-%m-%d %H:%M}"
    names = ", ".join(SYMBOLS)
    return [
        {"role": "system", "content": SYSTEM.format(instant=when, symbols=names)},
        {"role": "user", "content": TASK.format(symbols=names)},
    ]


def agent_config(endpoint: str) -> dict:
    return {
        "decider": AGENT_DECIDER,
        "endpoint": endpoint,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "max_steps": MAX_STEPS,
        "system": SYSTEM,
        "task": TASK,
        "tools": TOOLS,
        "features": list(AGENT_FEATURES),
        "symbols": list(SYMBOLS),
    }


def strategy_config(params: Params) -> dict:
    return {
        "decider": STRATEGY_DECIDER,
        "strategy": STRATEGY,
        "params": params.to_json(),
        "features": list(FEATURES),
        "symbols": list(SYMBOLS),
    }


def parse_decision(text: str) -> dict[str, str]:
    found = re.search(r"\{.*\}", text or "", re.S)
    if not found:
        raise ValueError("the answer has no JSON object")
    try:
        stances = json.loads(found.group(0))
    except ValueError as error:
        raise ValueError(f"the answer's JSON does not parse: {error}") from error
    if not isinstance(stances, dict):
        raise ValueError("the answer's JSON is not an object")
    extra = sorted(set(stances) - set(SYMBOLS))
    if extra:
        raise ValueError(f"the answer rates symbols outside the task: {extra}")
    decision = {}
    for name in SYMBOLS:
        stance = stances.get(name)
        stance = stance.strip().lower() if isinstance(stance, str) else stance
        if stance not in STANCES:
            raise ValueError(f"{name} is rated {stance!r}, not one of {', '.join(STANCES)}")
        decision[name] = stance
    return decision


@dataclass(frozen=True)
class Sample:
    decision: str | None
    input_hash: str
    calls: tuple[tuple[str, str], ...]
    model_calls: int
    error: str | None = None
    answer: str = ""
    seconds: float = 0.0

    @property
    def valid(self) -> bool:
        return self.decision is not None


def answer(tools: dict[str, Tool], name: str, arguments: dict) -> str:
    try:
        if name not in tools:
            raise ValueError(f"There is no tool called {name}.")
        text = tools[name](arguments)
    except ValueError as error:
        text = f"Error: {error}"
    return text[:MAX_RESULT]


def run_agent(complete: Complete, tools: dict[str, Tool], instant: datetime) -> Sample:
    messages = prompts(instant)
    inputs, calls, model_calls = [], [], 0
    for step in range(MAX_STEPS):
        reply = complete(messages, TOOLS if step < MAX_STEPS - 1 else None)
        model_calls += 1
        requested = reply.get("tool_calls") or []
        if not requested:
            text = reply.get("content") or ""
            try:
                decision, error = canonical(parse_decision(text)), None
            except ValueError as found:
                decision, error = None, str(found)
            return Sample(
                decision, fingerprint(inputs), tuple(calls), model_calls, error, text[:2000]
            )
        messages.append(
            {"role": "assistant", "content": reply.get("content") or "", "tool_calls": requested}
        )
        for call in requested:
            name = call["function"]["name"]
            try:
                arguments = json.loads(call["function"].get("arguments") or "{}")
            except ValueError:
                arguments = {}
            text = answer(tools, name, arguments)
            inputs.append([name, canonical(arguments), text])
            calls.append((name, canonical(arguments)))
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": text})
    return Sample(
        None, fingerprint(inputs), tuple(calls), model_calls, "no answer within the steps"
    )


def strategy_sample(rows: Iterable[tuple], times: list[datetime], params: Params) -> Sample:
    received = inputs_at(rows, times)
    seen = observations(r[:5] for r in received)
    result = run(seen, times, params)
    decision = {"weights": dict(sorted(result.weights.items())), "result": result.fingerprint}
    return Sample(canonical(decision), input_fingerprint(seen), (), 0)


def mode(decisions: Iterable[str | None]) -> str | None:
    counts = Counter(d for d in decisions if d is not None)
    if not counts:
        return None
    return min(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]


@dataclass(frozen=True)
class Summary:
    decision: str | None
    rounds: tuple[str | None, ...]
    agreement: float
    parts: dict[str, float]
    distinct: int
    invalid: int
    samples: int

    @property
    def settled(self) -> dict[str, object]:
        if self.decision is None:
            return {}
        return {k: v for k, v in json.loads(self.decision).items() if v != UNSETTLED}

    def reproducible(self, minimum: float) -> bool:
        stable = all(r == self.decision for r in self.rounds)
        valid = self.samples - self.invalid >= minimum * self.samples
        return self.decision is not None and bool(self.settled) and stable and valid


def settle(decisions: list[dict], keys: list[str], minimum: float) -> tuple[dict, dict]:
    settled, shares = {}, {}
    for key in keys:
        values = [canonical(d.get(key)) for d in decisions]
        top = mode(values)
        share = values.count(top) / len(values) if values else 0.0
        shares[key] = share
        settled[key] = json.loads(top) if values and share >= minimum else UNSETTLED
    return settled, shares


def summarize(rounds: list[list[Sample]], minimum: float) -> Summary:
    every = [s for found in rounds for s in found]
    valid = [json.loads(s.decision) for s in every if s.valid]
    invalid = len(every) - len(valid)
    if not valid:
        return Summary(None, tuple(None for _ in rounds), 0.0, {}, 0, invalid, len(every))
    keys = list(dict.fromkeys(k for d in valid for k in d))
    settled, shares = settle(valid, keys, minimum)
    by_round = []
    for found in rounds:
        mine = [json.loads(s.decision) for s in found if s.valid]
        if not mine:
            by_round.append(None)
            continue
        modes = {k: json.loads(mode(canonical(d.get(k)) for d in mine)) for k in keys}
        by_round.append(
            canonical({k: UNSETTLED if settled[k] == UNSETTLED else modes[k] for k in keys})
        )
    whole = mode(s.decision for s in every)
    return Summary(
        canonical(settled),
        tuple(by_round),
        sum(1 for s in every if s.decision == whole) / len(every),
        shares,
        len({s.decision for s in every if s.valid}),
        invalid,
        len(every),
    )

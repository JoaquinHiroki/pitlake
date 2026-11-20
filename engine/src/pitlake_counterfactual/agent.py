import json
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

from pitlake_counterfactual.world import Row, World, fingerprint, instant, micros

STANCES = ("overweight", "neutral", "underweight")
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
ANSWER_TOOL = "decide"
LOWEST = float("-inf")

Complete = Callable[[list[dict], list[dict] | None], dict]
Call = Callable[[list[dict], list[dict] | None, str | None], dict]


def canonical(payload) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _rank(row: Row) -> tuple:
    _, _, event_time, knowable_at, value, table, ref = row
    return (event_time, knowable_at, table or "", ref, LOWEST if value is None else value)


class View:
    def __init__(self, rows: Iterable[Row], at: datetime, features: Iterable[str] = AGENT_FEATURES):
        self.at = at
        cutoff = micros(at)
        features = set(features)
        self.events: dict[tuple[str, str], dict[int, Row]] = {}
        for row in rows:
            if row[1] not in features or row[3] > cutoff or row[4] is None:
                continue
            events = self.events.setdefault((row[0], row[1]), {})
            current = events.get(row[2])
            if current is None or _rank(row) > _rank(current):
                events[row[2]] = row

    def history(self, entity: str, feature: str) -> list[Row]:
        return [r for _, r in sorted(self.events.get((entity, feature), {}).items())]

    def latest(self, entity: str, feature: str) -> Row | None:
        found = self.history(entity, feature)
        return found[-1] if found else None


def stamp(us: int) -> str:
    return f"{instant(us):%Y-%m-%d %H:%M} UTC"


def snapshot_text(view: View, name: str) -> str:
    closes = view.history(name, "close")
    if not closes:
        return f"No close of {name} was public at the instant."
    values = [r[4] for r in closes]
    lines = [f"{name}: last close {values[-1]:,.2f} on {instant(closes[-1][2]):%Y-%m-%d}."]
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
            f"as of {instant(volatility[2]):%Y-%m-%d}."
        )
    for feature in ("revenue_q", "net_income_q", "eps_diluted_q"):
        row = view.latest(name, feature)
        if row:
            shown = f"{row[4]:,.2f}" if feature == "eps_diluted_q" else f"${row[4] / 1e6:,.0f}M"
            lines.append(
                f"{LABELS[feature]} ending {instant(row[2]):%Y-%m-%d}: {shown} "
                f"(public since {stamp(row[3])})."
            )
    return "\n".join(lines)


def history_text(view: View, name: str, window: int) -> str:
    closes = view.history(name, "close")
    if not closes:
        return f"No close of {name} was public at the instant."
    start = instant(closes[-1][2]) - timedelta(days=window)
    points = [(instant(r[2]), r[4]) for r in closes if instant(r[2]) > start]
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


def macro_text(view: View, entity: str) -> str:
    lines = []
    for feature in ("cpi_yoy", "unrate", "fedfunds"):
        row = view.latest(entity, feature)
        if row:
            shown = f"{row[4] * 100:.2f}%" if feature == "cpi_yoy" else f"{row[4]:.2f}%"
            lines.append(
                f"{LABELS[feature]} for {instant(row[2]):%Y-%m}: {shown} "
                f"(public since {stamp(row[3])})."
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


def tools_for(symbols: tuple[str, ...]) -> list[dict]:
    symbol = {"type": "string", "enum": list(symbols)}
    return [
        _tool(
            "symbol_snapshot",
            "Latest close, price moves over 1, 5, 20 and 60 sessions, 20-day volatility and the "
            "latest quarterly results of one symbol, each as public at the locked instant.",
            {"symbol": symbol},
            ["symbol"],
        ),
        _tool(
            "price_history",
            "Daily closes of one symbol over the given number of days before the locked instant.",
            {"symbol": symbol, "days": {"type": "integer", "minimum": DAYS[0], "maximum": DAYS[1]}},
            ["symbol", "days"],
        ),
        _tool(
            "macro",
            "US CPI inflation, unemployment and the fed funds rate public at the locked instant.",
            {},
            [],
        ),
    ]


def decide_tool(symbols: tuple[str, ...]) -> dict:
    return _tool(
        ANSWER_TOOL,
        "Give the rating of every symbol. Call it once, last, when you have the evidence.",
        {name: {"type": "string", "enum": list(STANCES)} for name in symbols},
        list(symbols),
    )


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


def prompts(at: datetime, symbols: tuple[str, ...]) -> list[dict]:
    when = f"{at:%Y-%m-%d %H:%M}"
    names = ", ".join(symbols)
    return [
        {"role": "system", "content": SYSTEM.format(instant=when, symbols=names)},
        {"role": "user", "content": TASK.format(symbols=names)},
    ]


def agent_config(model: str, symbols: tuple[str, ...], answer_by_tool: bool = False) -> dict:
    config = {
        "decider": "agent",
        "endpoint": model,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "max_steps": MAX_STEPS,
        "system": SYSTEM,
        "task": TASK,
        "tools": tools_for(symbols),
        "features": list(AGENT_FEATURES),
        "symbols": list(symbols),
    }
    if answer_by_tool:
        config["answer"] = decide_tool(symbols)
    return config


def answered_by_tool(call: Call, symbols: tuple[str, ...]) -> Complete:
    tool = decide_tool(symbols)

    def complete(messages: list[dict], tools: list[dict] | None) -> dict:
        if not tools and not any(m["role"] == "tool" for m in messages):
            return call(messages, None, None)
        reply = call(messages, [*(tools or []), tool], "required")
        for requested in reply.get("tool_calls") or []:
            if requested["function"]["name"] == ANSWER_TOOL:
                return {"role": "assistant", "content": requested["function"].get("arguments")}
        return reply

    return complete


def parse_decision(text, symbols: tuple[str, ...]) -> dict[str, str]:
    if isinstance(text, dict):
        stances = text
    else:
        found = re.search(r"\{.*\}", text or "", re.S)
        if not found:
            raise ValueError("the answer has no JSON object")
        try:
            stances = json.loads(found.group(0))
        except ValueError as error:
            raise ValueError(f"the answer's JSON does not parse: {error}") from error
    if not isinstance(stances, dict):
        raise ValueError("the answer's JSON is not an object")
    extra = sorted(set(stances) - set(symbols))
    if extra:
        raise ValueError(f"the answer rates symbols outside the task: {extra}")
    decision = {}
    for name in symbols:
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

    def to_json(self) -> dict:
        return {
            "decision": self.decision,
            "input_hash": self.input_hash,
            "calls": [list(c) for c in self.calls],
            "model_calls": self.model_calls,
            "error": self.error,
            "answer": self.answer,
            "seconds": round(self.seconds, 3),
        }


def symbol_of(arguments: dict, symbols: tuple[str, ...]) -> str:
    found = str(arguments.get("symbol", ""))
    if found not in symbols:
        raise ValueError(f"No symbol {found!r}; choose one of {', '.join(symbols)}.")
    return found


def days_of(arguments: dict) -> int:
    try:
        found = int(arguments.get("days", 60))
    except (TypeError, ValueError):
        found = 60
    return max(DAYS[0], min(DAYS[1], found))


@dataclass
class Session:
    world: World
    view: View
    calls: list[tuple[str, str]] = field(default_factory=list)
    inputs: list[list[str]] = field(default_factory=list)
    model_calls: int = 0

    @classmethod
    def of(cls, world: World) -> "Session":
        return cls(world, View(world.rows, world.at))

    @property
    def at(self) -> datetime:
        return self.world.at

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.world.bundle.symbols

    @property
    def tools(self) -> list[dict]:
        return tools_for(self.symbols)

    def answer(self, name: str, arguments: dict) -> str:
        try:
            if name == "symbol_snapshot":
                text = snapshot_text(self.view, symbol_of(arguments, self.symbols))
            elif name == "price_history":
                text = history_text(
                    self.view, symbol_of(arguments, self.symbols), days_of(arguments)
                )
            elif name == "macro":
                text = macro_text(self.view, self.world.bundle.global_entity)
            else:
                raise ValueError(f"There is no tool called {name}.")
        except ValueError as error:
            text = f"Error: {error}"
        return text[:MAX_RESULT]

    def call(self, name: str, arguments: dict | None = None) -> str:
        arguments = dict(arguments or {})
        text = self.answer(name, arguments)
        self.inputs.append([name, canonical(arguments), text])
        self.calls.append((name, canonical(arguments)))
        return text

    def sample(self, answer, error: str | None = None) -> Sample:
        decision = None
        if error is None:
            try:
                decision = canonical(parse_decision(answer, self.symbols))
            except ValueError as found:
                error = str(found)
        text = answer if isinstance(answer, str) else canonical(answer) if answer else ""
        return Sample(
            decision,
            fingerprint(self.inputs),
            tuple(self.calls),
            self.model_calls,
            error,
            (text or "")[:2000],
        )


Decider = Callable[[Session], object]


def loop(complete: Complete) -> Decider:
    def decide(session: Session):
        messages = prompts(session.at, session.symbols)
        for step in range(MAX_STEPS):
            reply = complete(messages, session.tools if step < MAX_STEPS - 1 else None)
            session.model_calls += 1
            requested = reply.get("tool_calls") or []
            if not requested:
                return reply.get("content") or ""
            messages.append(
                {
                    "role": "assistant",
                    "content": reply.get("content") or "",
                    "tool_calls": requested,
                }
            )
            for call in requested:
                try:
                    arguments = json.loads(call["function"].get("arguments") or "{}")
                except ValueError:
                    arguments = {}
                if not isinstance(arguments, dict):
                    arguments = {}
                text = session.call(call["function"]["name"], arguments)
                messages.append({"role": "tool", "tool_call_id": call["id"], "content": text})
        raise NoAnswer("no answer within the steps")

    return decide


class NoAnswer(Exception):
    pass


def run(decider: Decider, world: World) -> Sample:
    session = Session.of(world)
    started = time.perf_counter()
    try:
        sample = session.sample(decider(session))
    except Exception as error:
        message = str(error) if isinstance(error, NoAnswer) else f"{type(error).__name__}: {error}"
        sample = session.sample(None, message)
    return replace(sample, seconds=time.perf_counter() - started)

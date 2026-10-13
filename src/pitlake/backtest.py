import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta

STRATEGY = "trend_vol_target"
DECISION_HOUR = 6
MAX_LEVERAGE = 2.0
COST_BPS = 5.0
INFLATION_BRAKE = 0.03
BRAKE_FACTOR = 0.5
PERIODS_PER_YEAR = 365
FEATURES = ("close", "volatility_20d", "net_income_q", "cpi_yoy")


@dataclass(frozen=True)
class Asset:
    entity: str
    periods_per_year: int
    has_fundamentals: bool


UNIVERSE = (
    Asset("BTCUSDT", 365, False),
    Asset("SPY", 252, False),
    Asset("AAPL", 252, True),
    Asset("MSFT", 252, True),
    Asset("NVDA", 252, True),
)


@dataclass(frozen=True)
class Params:
    lookback: int
    target_vol: float
    macro_brake: bool
    require_profit: bool
    cost_bps: float = COST_BPS

    def __post_init__(self) -> None:
        if self.lookback < 1:
            raise ValueError("lookback must be at least 1 observation")
        if not 0 < self.target_vol <= 1:
            raise ValueError("target_vol must be in (0, 1]")
        if self.cost_bps < 0:
            raise ValueError("cost_bps must not be negative")

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> "Params":
        return cls(**json.loads(text))

    @property
    def label(self) -> str:
        brake = "brake" if self.macro_brake else "nobrake"
        profit = "profit" if self.require_profit else "any"
        return f"lb{self.lookback}-vol{self.target_vol:g}-{brake}-{profit}"


GRID = tuple(
    Params(lookback, target_vol, brake, profit)
    for lookback in (20, 60, 120)
    for target_vol in (0.1, 0.2)
    for brake in (False, True)
    for profit in (False, True)
)


@dataclass(frozen=True)
class Day:
    at: datetime
    pnl: float
    cost: float
    gross: float
    turnover: float

    @property
    def net(self) -> float:
        return self.pnl - self.cost


@dataclass(frozen=True)
class Result:
    days: tuple[Day, ...]
    metrics: dict[str, float]
    fingerprint: str


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def decision_times(start: date, end: date) -> list[datetime]:
    if end < start:
        raise ValueError(f"the period ends ({end}) before it starts ({start})")
    return [
        datetime.combine(start + timedelta(days=i), time(DECISION_HOUR), UTC)
        for i in range((end - start).days + 1)
    ]


Observations = dict[tuple[str, datetime], dict[str, tuple[float, datetime]]]


def observations(rows: Iterable[tuple]) -> Observations:
    seen: Observations = {}
    for entity, at, feature, value, event_time in rows:
        if value is None:
            continue
        seen.setdefault((entity, as_utc(at)), {})[feature] = (float(value), as_utc(event_time))
    return seen


def fingerprint(payload) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def input_fingerprint(seen: Observations) -> str:
    return fingerprint(
        sorted(
            [entity, at.isoformat(), feature, value, event_time.isoformat()]
            for (entity, at), features in seen.items()
            for feature, (value, event_time) in features.items()
        )
    )


def target_weight(asset: Asset, seen: dict, closes: list, params: Params, assets: int) -> float:
    if len(closes) <= params.lookback:
        return 0.0
    momentum = closes[-1][1] / closes[-1 - params.lookback][1] - 1
    volatility = seen.get("volatility_20d")
    if momentum <= 0 or volatility is None or volatility[0] <= 0:
        return 0.0
    if params.require_profit and asset.has_fundamentals:
        income = seen.get("net_income_q")
        if income is None or income[0] <= 0:
            return 0.0
    annual_volatility = volatility[0] * math.sqrt(asset.periods_per_year)
    size = min(params.target_vol / annual_volatility, MAX_LEVERAGE)
    inflation = seen.get("cpi_yoy")
    if params.macro_brake and inflation is not None and inflation[0] > INFLATION_BRAKE:
        size *= BRAKE_FACTOR
    return size / assets


def run(
    seen: Observations,
    times: list[datetime],
    params: Params,
    universe: tuple[Asset, ...] = UNIVERSE,
) -> Result:
    weights = {a.entity: 0.0 for a in universe}
    closes: dict[str, list] = {a.entity: [] for a in universe}
    days = []
    for k, at in enumerate(times):
        pnl = 0.0
        if k:
            for asset in universe:
                before = seen.get((asset.entity, times[k - 1]), {}).get("close")
                after = seen.get((asset.entity, at), {}).get("close")
                if weights[asset.entity] and before and after:
                    pnl += weights[asset.entity] * (after[0] / before[0] - 1)
        targets = {}
        for asset in universe:
            known = seen.get((asset.entity, at), {})
            close = known.get("close")
            history = closes[asset.entity]
            if close is not None and (not history or history[-1][0] != close[1]):
                history.append((close[1], close[0]))
            targets[asset.entity] = target_weight(asset, known, history, params, len(universe))
        turnover = sum(abs(targets[e] - weights[e]) for e in targets)
        cost = turnover * params.cost_bps / 10000
        days.append(Day(at, pnl, cost, sum(abs(w) for w in targets.values()), turnover))
        weights = targets
    metrics = summarize(days)
    return Result(tuple(days), metrics, result_fingerprint(days, metrics))


def summarize(days: Iterable[Day], periods: int = PERIODS_PER_YEAR) -> dict[str, float]:
    days = list(days)
    returns = [d.net for d in days]
    n = len(returns)
    equity, peak, drawdown = 1.0, 1.0, 0.0
    for r in returns:
        equity *= 1 + r
        peak = max(peak, equity)
        drawdown = max(drawdown, 1 - equity / peak)
    mean = sum(returns) / n if n else 0.0
    std = math.sqrt(sum((r - mean) ** 2 for r in returns) / (n - 1)) if n > 1 else 0.0
    return {
        "total_return": equity - 1,
        "annual_return": equity ** (periods / n) - 1 if n and equity > 0 else 0.0,
        "annual_volatility": std * math.sqrt(periods),
        "sharpe": mean / std * math.sqrt(periods) if std > 0 else 0.0,
        "max_drawdown": drawdown,
        "days": float(n),
        "average_gross": sum(d.gross for d in days) / n if n else 0.0,
        "turnover": sum(d.turnover for d in days),
    }


def daily_rows(days: Iterable[Day]) -> list[list]:
    return [[d.at.isoformat(), d.pnl, d.cost, d.net, d.gross, d.turnover] for d in days]


def result_fingerprint(days: Iterable[Day], metrics: dict[str, float]) -> str:
    return fingerprint({"days": daily_rows(days), "metrics": metrics})


def differences(recorded: dict, input_hash: str, result: Result) -> list[str]:
    found = []
    if recorded["input_hash"] != input_hash:
        found.append(f"input {recorded['input_hash'][:12]} -> {input_hash[:12]}")
    if recorded["result_hash"] != result.fingerprint:
        found.append(f"result {recorded['result_hash'][:12]} -> {result.fingerprint[:12]}")
    for name, value in sorted(recorded["metrics"].items()):
        if result.metrics.get(name) != value:
            found.append(f"{name} {value!r} -> {result.metrics.get(name)!r}")
    return found

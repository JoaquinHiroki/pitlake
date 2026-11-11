import json
from collections.abc import Iterable
from dataclasses import dataclass
from math import comb

from pitlake.backtest import fingerprint
from pitlake.harness import UNSETTLED, Sample, Summary, Tool, answer, settle
from pitlake.information import BAR, Item

ALPHA = 0.05
SAME = "same"
CHANGED = "changed"
FLIPPED = "flipped"
INVALID = "invalid"
NULL = "null"
EVALUATED = "evaluated"
SCREENED = "screened"
SAMPLED = "sampled"
VERIFY_CHANGED = "changed"
VERIFY_ALL = "all"
VERIFY_NONE = "none"
VERIFY = (VERIFY_CHANGED, VERIFY_ALL, VERIFY_NONE)
BARS = 3


def fisher(agree: int, total: int, other_agree: int, other_total: int) -> float:
    successes, population = agree + other_agree, total + other_total
    if not total or not other_total:
        return 1.0
    low = max(0, total - (population - successes))
    high = min(successes, total)
    denominator = comb(population, total)

    def probability(x: int) -> float:
        return comb(successes, x) * comb(population - successes, total - x) / denominator

    observed = probability(agree)
    found = sum(
        p for p in (probability(x) for x in range(low, high + 1)) if p <= observed * (1 + 1e-7)
    )
    return min(1.0, found)


@dataclass(frozen=True)
class Base:
    decision: str | None
    settled: dict[str, object]
    support: dict[str, int]
    valid: int
    calls: tuple[tuple[str, str], ...]
    input_hash: str
    reproducible: bool

    def share(self, part: str) -> float:
        return self.support[part] / self.valid if self.valid else 0.0


def weights(decision: str) -> dict[str, float]:
    return dict(json.loads(decision)["weights"])


def strategy_base(samples: list[Sample]) -> Base:
    first = samples[0]
    stable = all(s.decision == first.decision and s.valid for s in samples)
    settled = weights(first.decision) if first.valid else {}
    return Base(
        first.decision,
        settled,
        {k: 1 for k in settled},
        1,
        (),
        first.input_hash,
        stable and bool(settled),
    )


def calls_of(samples: Iterable[Sample]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted({c for s in samples for c in s.calls}))


def tool_inputs(tools: dict[str, Tool], calls: Iterable[tuple[str, str]]) -> list[list[str]]:
    return [
        [name, arguments, answer(tools, name, json.loads(arguments))] for name, arguments in calls
    ]


def inputs_hash(tools: dict[str, Tool], calls: Iterable[tuple[str, str]]) -> str:
    return fingerprint(tool_inputs(tools, calls))


def agent_base(summary: Summary, samples: list[Sample], minimum: float, input_hash: str) -> Base:
    valid = [json.loads(s.decision) for s in samples if s.valid]
    settled = summary.settled
    support = {k: sum(1 for d in valid if d.get(k) == v) for k, v in settled.items()}
    return Base(
        summary.decision,
        settled,
        support,
        len(valid),
        calls_of(samples),
        input_hash,
        summary.reproducible(minimum),
    )


@dataclass(frozen=True)
class Effect:
    outcomes: dict[str, str]
    effects: dict[str, float]
    p_values: dict[str, float]
    caused: tuple[str, ...]
    valid: bool = True

    @property
    def changed(self) -> bool:
        return any(o != SAME for o in self.outcomes.values())


def unchanged(base: Base) -> Effect:
    return Effect({k: SAME for k in base.settled}, {k: 0.0 for k in base.settled}, {}, ())


def strategy_effect(base: Base, decision: str) -> Effect:
    found = weights(decision)
    outcomes, effects = {}, {}
    for part, weight in base.settled.items():
        now = found.get(part, 0.0)
        outcomes[part] = SAME if now == weight else CHANGED
        effects[part] = abs(now - weight)
    caused = tuple(sorted(k for k, o in outcomes.items() if o != SAME))
    return Effect(outcomes, effects, {}, caused)


def agent_effect(base: Base, samples: list[Sample], minimum: float) -> Effect:
    valid = [json.loads(s.decision) for s in samples if s.valid]
    if not valid or len(valid) < minimum * len(samples):
        parts = list(base.settled)
        return Effect({k: INVALID for k in parts}, {k: 0.0 for k in parts}, {}, (), False)
    outcomes, effects, p_values = {}, {}, {}
    for part, value in base.settled.items():
        agree = sum(1 for d in valid if d.get(part) == value)
        now = settle(valid, [part], minimum)[0][part]
        outcomes[part] = SAME if now == value else UNSETTLED if now == UNSETTLED else FLIPPED
        effects[part] = base.share(part) - agree / len(valid)
        p_values[part] = fisher(base.support[part], base.valid, agree, len(valid))
    caused = tuple(sorted(k for k, o in outcomes.items() if o != SAME and p_values[k] < ALPHA))
    return Effect(outcomes, effects, p_values, caused)


def null_passed(effect: Effect) -> bool:
    quiet = all(p >= ALPHA for p in effect.p_values.values())
    return effect.valid and not effect.changed and not effect.caused and quiet


def spread(found: list, count: int) -> list:
    if count <= 0 or len(found) <= count:
        return list(found)
    if count == 1:
        return [found[-1]]
    return [
        found[k] for k in sorted({round(k * (len(found) - 1) / (count - 1)) for k in range(count)})
    ]


def select(changed: Iterable[Item], bars: int) -> list[Item]:
    chosen, by_symbol = [], {}
    for item in sorted(changed, key=lambda i: (i.knowable_at, i.id)):
        if item.kind == BAR:
            by_symbol.setdefault(item.entity, []).append(item)
        else:
            chosen.append(item)
    for symbol in sorted(by_symbol):
        chosen += spread(by_symbol[symbol], bars)
    return sorted(chosen, key=lambda i: (i.knowable_at, i.id))

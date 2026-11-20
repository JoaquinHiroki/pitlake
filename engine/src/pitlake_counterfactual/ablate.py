import json
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from math import comb

from pitlake_counterfactual.agent import Decider, Sample, Session, canonical, run
from pitlake_counterfactual.world import Bundle, World, fingerprint

ALPHA = 0.05
UNSETTLED = "unsettled"
SAME = "same"
FLIPPED = "flipped"
INVALID = "invalid"
CHANGED = "CHANGED"
UNCHANGED = "SAME"
UNCLEAR = "UNCLEAR"
BROKEN = "INVALID"


def mode(decisions: Iterable[str | None]) -> str | None:
    counts = Counter(d for d in decisions if d is not None)
    if not counts:
        return None
    return min(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]


def settle(decisions: list[dict], keys: list[str], minimum: float) -> tuple[dict, dict]:
    settled, shares = {}, {}
    for key in keys:
        values = [canonical(d.get(key)) for d in decisions]
        top = mode(values)
        share = values.count(top) / len(values) if values else 0.0
        shares[key] = share
        settled[key] = json.loads(top) if values and share >= minimum else UNSETTLED
    return settled, shares


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
    parts: dict[str, float]
    support: dict[str, int]
    valid: int
    samples: int
    stable: bool
    calls: tuple[tuple[str, str], ...]

    @property
    def reproducible(self) -> bool:
        return self.decision is not None and bool(self.settled) and self.stable

    def share(self, part: str) -> float:
        return self.support[part] / self.valid if self.valid else 0.0


def base_of(rounds: list[list[Sample]], minimum: float) -> Base:
    every = [s for found in rounds for s in found]
    valid = [json.loads(s.decision) for s in every if s.valid]
    calls = tuple(sorted({c for s in every for c in s.calls}))
    if not valid:
        return Base(None, {}, {}, {}, 0, len(every), False, calls)
    keys = list(dict.fromkeys(k for d in valid for k in d))
    settled, shares = settle(valid, keys, minimum)
    stable = len(valid) >= minimum * len(every)
    for found in rounds:
        mine = [json.loads(s.decision) for s in found if s.valid]
        if not mine:
            stable = False
            continue
        for k in keys:
            if (
                settled[k] != UNSETTLED
                and json.loads(mode(canonical(d.get(k)) for d in mine)) != (settled[k])
            ):
                stable = False
    kept = {k: v for k, v in settled.items() if v != UNSETTLED}
    support = {k: sum(1 for d in valid if d.get(k) == v) for k, v in kept.items()}
    return Base(canonical(settled), kept, shares, support, len(valid), len(every), stable, calls)


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

    @property
    def quiet(self) -> bool:
        return all(p >= ALPHA for p in self.p_values.values())

    def to_json(self) -> dict:
        return {
            "outcomes": self.outcomes,
            "effects": self.effects,
            "p_values": self.p_values,
            "caused": list(self.caused),
            "valid": self.valid,
        }


def effect_of(base: Base, samples: list[Sample], minimum: float) -> Effect:
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


def verdict(world: World, effect: Effect) -> str:
    if not world.coherent or not effect.valid:
        return BROKEN
    if effect.caused:
        return CHANGED
    if not effect.changed and effect.quiet:
        return UNCHANGED
    return UNCLEAR


def inputs_changed(base: World, world: World, calls: Iterable[tuple[str, str]]) -> bool:
    before, after = Session.of(base), Session.of(world)
    return any(
        before.answer(name, json.loads(arguments)) != after.answer(name, json.loads(arguments))
        for name, arguments in calls
    )


def draw(decider: Decider, world: World, count: int, workers: int) -> list[Sample]:
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        return list(pool.map(lambda _: run(decider, world), range(count)))


@dataclass(frozen=True)
class Tested:
    name: str
    world: World
    samples: list[Sample]
    effect: Effect
    inputs_changed: bool

    @property
    def verdict(self) -> str:
        return verdict(self.world, self.effect)

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "withheld": list(self.world.withheld),
            "world_hash": self.world.hash,
            "inputs_hash": self.world.inputs_hash(),
            "rows": len(self.world.rows),
            "rows_removed": self.world.removed,
            "rows_recomputed": self.world.recomputed,
            "coherent": self.world.coherent,
            "problems": list(self.world.problems),
            "inputs_changed": self.inputs_changed,
            "effect": self.effect.to_json(),
            "verdict": self.verdict,
            "samples": [s.to_json() for s in self.samples],
        }


@dataclass(frozen=True)
class Report:
    bundle: Bundle
    config: dict
    samples: int
    min_agreement: float
    problems: tuple[str, ...]
    base: Base
    base_rounds: list[list[Sample]]
    null: Tested | None
    worlds: list[Tested]

    @property
    def null_passed(self) -> bool | None:
        if self.null is None:
            return None
        return self.null.verdict == UNCHANGED

    @property
    def passed(self) -> bool:
        return (
            not self.problems
            and self.base.reproducible
            and self.null_passed is not False
            and all(t.world.coherent and t.effect.valid for t in self.worlds)
        )

    def to_json(self, version: str) -> dict:
        return {
            "engine": version,
            "bundle": {
                "name": self.bundle.name,
                "id": self.bundle.id,
                "set_hash": self.bundle.set_hash,
                "at": self.bundle.at.isoformat(),
                "items": self.bundle.by_kind(),
                "source": self.bundle.source,
                "problems": list(self.problems),
            },
            "config": self.config,
            "config_hash": fingerprint(self.config),
            "samples": self.samples,
            "min_agreement": self.min_agreement,
            "base": {
                "decision": self.base.decision,
                "settled": self.base.settled,
                "parts": self.base.parts,
                "support": self.base.support,
                "valid": self.base.valid,
                "samples": self.base.samples,
                "reproducible": self.base.reproducible,
                "calls": [list(c) for c in self.base.calls],
                "rounds": [[s.to_json() for s in r] for r in self.base_rounds],
            },
            "null": None if self.null is None else self.null.to_json(),
            "null_passed": self.null_passed,
            "worlds": [t.to_json() for t in self.worlds],
            "passed": self.passed,
        }


def ablate(
    bundle: Bundle,
    worlds: list[tuple[str, ...]],
    decider: Decider,
    config: dict,
    samples: int = 10,
    min_agreement: float = 0.8,
    workers: int = 2,
    rounds: int = 2,
    null: bool = True,
) -> Report:
    problems = tuple(bundle.check())
    real = bundle.withhold(())
    base_rounds = [draw(decider, real, samples, workers) for _ in range(rounds)]
    base = base_of(base_rounds, min_agreement)

    def test(name: str, world: World) -> Tested:
        found = draw(decider, world, samples, workers)
        changed = inputs_changed(real, world, base.calls)
        return Tested(name, world, found, effect_of(base, found, min_agreement), changed)

    tested_null = test("null", real) if null else None
    tested = [test(" ".join(w), bundle.withhold(w)) for w in worlds]
    return Report(
        bundle, config, samples, min_agreement, problems, base, base_rounds, tested_null, tested
    )

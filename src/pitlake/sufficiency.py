import json
from collections import Counter, deque
from collections.abc import Callable, Iterable

from pitlake.attribution import Base
from pitlake.backtest import fingerprint
from pitlake.harness import Sample
from pitlake.information import BAR, KINDS, Item

SEARCH = "search"
MINIMAL = "minimal"
VERIFY_FINAL = "final"
VERIFY_ALL = "all"
VERIFY = (VERIFY_FINAL, VERIFY_ALL)

Passes = Callable[[frozenset[str], tuple[str, ...], str], bool]


def order(item: Item) -> tuple:
    return (item.knowable_at, item.id)


def kept_hash(kept: Iterable[str]) -> str:
    return fingerprint(sorted(kept))


def split(chunk: list[Item]) -> list[list[Item]]:
    for key in (lambda i: KINDS.index(i.kind), lambda i: i.entity):
        groups: dict = {}
        for item in chunk:
            groups.setdefault(key(item), []).append(item)
        if len(groups) > 1:
            return [sorted(groups[k], key=order) for k in sorted(groups)]
    ordered = sorted(chunk, key=order)
    half = len(ordered) // 2
    return [ordered[:half], ordered[half:]]


def reduce(items: Iterable[Item], passes: Passes) -> frozenset[str]:
    items = sorted(items, key=order)
    kept = frozenset(i.id for i in items)
    queue = deque([items])
    while queue:
        chunk = [i for i in queue.popleft() if i.id in kept]
        if not chunk:
            continue
        dropped = tuple(i.id for i in chunk)
        trial = kept - set(dropped)
        if passes(trial, dropped, SEARCH):
            kept = trial
        elif len(chunk) > 1:
            queue.extend(split(chunk))
    rank = {i.id: order(i) for i in items}
    settled = False
    while not settled:
        settled = True
        for found in sorted(kept, key=rank.__getitem__):
            trial = kept - {found}
            if passes(trial, (found,), MINIMAL):
                kept, settled = trial, False
    return kept


def hopeless(base: Base, samples: list[Sample], total: int, minimum: float) -> bool:
    left = total - len(samples)
    valid = [json.loads(s.decision) for s in samples if s.valid]
    if len(valid) + left < minimum * total:
        return True
    for part, value in base.settled.items():
        agree = sum(1 for d in valid if d.get(part) == value)
        reachable = len(valid) + left
        if not reachable or (agree + left) / reachable < minimum:
            return True
    return False


def by_kind(kept: Iterable[str]) -> dict[str, int]:
    counts = Counter(k.split(":", 1)[0] for k in kept)
    return {kind: counts.get(kind, 0) for kind in KINDS}


def outline(items: Iterable[Item]) -> list[str]:
    groups: dict[tuple[str, str], list[Item]] = {}
    for item in items:
        groups.setdefault((item.entity, item.kind), []).append(item)
    lines = []
    for (entity, kind), found in sorted(groups.items()):
        found.sort(key=order)
        if kind == BAR:
            first, last = (i.key.split("/")[1] for i in (found[0], found[-1]))
            span = first if first == last else f"{first} to {last}"
            lines.append(f"{entity}: {len(found)} bar{'s' if len(found) > 1 else ''} {span}")
        else:
            lines.append(f"{entity}: {' '.join(i.id for i in found)}")
    return lines

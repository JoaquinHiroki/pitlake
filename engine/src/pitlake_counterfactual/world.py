import gzip
import hashlib
import json
import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

FORMAT = "pitlake-world/1"
BAR = "bar"
FILING = "filing"
RELEASE = "release"
KINDS = (BAR, FILING, RELEASE)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
TOLERANCE = 1e-9
SAMPLE = 20

Row = tuple


def fingerprint(payload) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def instant(micros: int) -> datetime:
    return EPOCH + timedelta(microseconds=micros)


def micros(at: datetime) -> int:
    at = at.replace(tzinfo=UTC) if at.tzinfo is None else at.astimezone(UTC)
    return (at - EPOCH) // timedelta(microseconds=1)


def day_micros(day: str) -> int:
    return micros(datetime.fromisoformat(day).replace(tzinfo=UTC))


def iso(us: int) -> str:
    return instant(us).isoformat()


@dataclass(frozen=True)
class Item:
    kind: str
    key: str
    entity: str
    knowable_at: int
    digest: str

    @property
    def id(self) -> str:
        return f"{self.kind}:{self.key}"


@dataclass(frozen=True)
class Bar:
    venue: str
    symbol: str
    day: str
    prices: tuple
    knowable_at: int

    @property
    def id(self) -> str:
        return f"{BAR}:{self.symbol}/{self.day}"

    @property
    def close(self) -> float:
        return float(self.prices[3])

    @property
    def volume(self) -> float:
        return float(self.prices[4])


@dataclass(frozen=True)
class Lineage:
    prices: str
    filings: str
    releases: str
    window: int
    price_features: tuple[str, ...]
    series_of: dict[str, str]
    levels: dict[str, str]

    @classmethod
    def of(cls, found: dict) -> "Lineage":
        return cls(
            found["prices"],
            found["filings"],
            found["releases"],
            int(found["window_days"]),
            tuple(found["price_features"]),
            dict(found["series_of"]),
            dict(found["levels"]),
        )

    def owner(self, row: Row) -> str:
        entity, feature, _, _, _, table, ref = row
        if table == self.prices:
            return f"{BAR}:{entity}/{ref}"
        if table == self.filings:
            return f"{FILING}:{ref}"
        if table == self.releases and feature in self.series_of:
            return f"{RELEASE}:{self.series_of[feature]}/{ref}"
        raise ValueError(f"no item owns {feature} of {entity} from {table} {ref}")


def bar_items(bars: Iterable[Bar]) -> list[Item]:
    found: dict[str, Item] = {}
    for b in bars:
        item = Item(
            BAR,
            f"{b.symbol}/{b.day}",
            b.symbol,
            b.knowable_at,
            fingerprint([b.venue, b.symbol, b.day, *b.prices, iso(b.knowable_at)]),
        )
        if item.id in found:
            raise ValueError(f"two daily bars are {item.id}; a symbol must trade on one venue")
        found[item.id] = item
    return list(found.values())


def filing_items(rows: Iterable[list]) -> list[Item]:
    grouped: dict[str, list[list]] = {}
    for row in rows:
        grouped.setdefault(row[1], []).append(row)
    found = []
    for accession, reported in sorted(grouped.items()):
        symbols = {r[0] for r in reported}
        if len(symbols) > 1:
            raise ValueError(f"filing {accession} is reported for {sorted(symbols)}")
        content = sorted(
            [*(None if v is None else str(v) for v in r[2:-1]), iso(r[-1])] for r in reported
        )
        times = [r[-1] for r in reported]
        found.append(Item(FILING, accession, reported[0][0], min(times), fingerprint(content)))
    return found


def release_items(rows: Iterable[list], levels: dict[str, str], entity: str) -> list[Item]:
    grouped: dict[tuple[str, str], list[tuple]] = {}
    for feature, event_time, knowable_at, value, vintage in rows:
        if feature not in levels:
            raise ValueError(f"{feature} is not the level of a series")
        grouped.setdefault((levels[feature], vintage), []).append((event_time, knowable_at, value))
    return [
        Item(
            RELEASE,
            f"{series}/{vintage}",
            entity,
            min(r[1] for r in observed),
            fingerprint(sorted([iso(e), v] for e, _, v in observed)),
        )
        for (series, vintage), observed in sorted(grouped.items())
    ]


def set_hash(items: Iterable[Item]) -> str:
    return fingerprint(sorted([i.id, iso(i.knowable_at), i.digest] for i in items))


def welford_stddev(values: list[float]) -> float | None:
    n, mean, m2 = 0.0, 0.0, 0.0
    for x in values:
        n += 1.0
        delta = x - mean
        step = delta / n
        mean += step
        m2 += delta * (delta - step)
    return math.sqrt(m2 / (n - 1.0)) if n > 1 else None


def average(values: list[float]) -> float:
    total = 0.0
    for x in values:
        total += x
    return total / len(values)


def price_rows(bars: list[Bar], lineage: Lineage) -> list[Row]:
    window = lineage.window
    bars = sorted(bars, key=lambda b: b.day)
    closes = [b.close for b in bars]
    returns = [None] + [math.log(closes[i] / closes[i - 1]) for i in range(1, len(bars))]
    found = []
    for i, b in enumerate(bars):
        knowable = max(x.knowable_at for x in bars[max(0, i - window) : i + 1])
        values = {"close": closes[i], "return_1d": returns[i]}
        if i >= window:
            recent = range(i - window + 1, i + 1)
            values["volatility_20d"] = welford_stddev([returns[k] for k in recent])
            values["dollar_volume_20d"] = average([closes[k] * bars[k].volume for k in recent])
        for feature in lineage.price_features:
            value = values.get(feature)
            if value is not None:
                found.append(
                    (b.symbol, feature, day_micros(b.day), knowable, value, lineage.prices, b.day)
                )
    return found


def near(a, b) -> bool:
    if a is None or b is None:
        return a is b
    return a == b or abs(a - b) <= TOLERANCE * max(abs(a), abs(b))


def differences(world: Iterable[Row], reference: Iterable[Row]) -> list[str]:
    def keyed(rows):
        found: dict[tuple, list] = {}
        for r in rows:
            found.setdefault((*r[:4], *r[5:]), []).append(r[4])
        return found

    have, want = keyed(world), keyed(reference)
    problems = []
    for key in sorted(set(have) | set(want), key=repr):
        mine, theirs = sorted(have.get(key, []), key=repr), sorted(want.get(key, []), key=repr)
        if len(mine) != len(theirs):
            entity, feature, event_time = key[0], key[1], iso(key[2])[:10]
            problems.append(
                f"{feature} of {entity} for {event_time} ({key[4]} {key[5]}): "
                f"{len(mine)} rows, expected {len(theirs)}"
            )
            continue
        for a, b in zip(mine, theirs, strict=True):
            if not near(a, b):
                problems.append(f"{key[1]} of {key[0]} for {key[5]} is {a!r}, expected {b!r}")
    return problems


def _canonical_rows(rows: Iterable[Row]) -> list[list]:
    return sorted(([*r] for r in rows), key=repr)


@dataclass(frozen=True)
class World:
    bundle: "Bundle"
    withheld: tuple[str, ...]
    rows: tuple[Row, ...]
    removed: int
    recomputed: int
    problems: tuple[str, ...] = field(default=())

    @property
    def coherent(self) -> bool:
        return not self.problems

    @property
    def hash(self) -> str:
        return fingerprint(_canonical_rows(self.rows))

    @property
    def at(self) -> datetime:
        return self.bundle.at

    def bars(self) -> list[Bar]:
        gone = set(self.withheld)
        return [b for b in self.bundle.bars if b.id not in gone]

    def inputs_hash(self) -> str:
        return inputs_hash(self.rows, self.bundle.symbols, self.bundle.global_entity, self.at)

    def export(self, folder: Path, name: str) -> Path:
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        meta = {
            "backtest_id": self.bundle.name,
            "ablation_id": name,
            "version": self.bundle.id[:16],
            "period_start": f"{self.at:%Y-%m-%d}",
            "period_end": f"{self.at:%Y-%m-%d}",
            "input_hash": self.inputs_hash(),
            "universe": ",".join(self.bundle.symbols),
            "global_entity": self.bundle.global_entity,
            "decision_hour": str(self.at.hour),
        }
        (folder / "meta.tsv").write_text("".join(f"{k}\t{v}\n" for k, v in meta.items()))

        def text(v) -> str:
            return "" if v is None else repr(v) if isinstance(v, float) else str(v)

        (folder / "features.tsv").write_text(
            "".join("\t".join(text(v) for v in r) + "\n" for r in _canonical_rows(self.rows))
        )
        (folder / "bars.tsv").write_text(
            "".join(
                f"{b.symbol}\t{b.day}\t{b.prices[3]}\t{b.prices[4]}\t{b.knowable_at}\n"
                for b in sorted(self.bars(), key=lambda b: (b.symbol, b.day))
            )
        )
        (folder / "withheld.tsv").write_text("".join(f"{i}\n" for i in self.withheld) or "\n")
        return folder


def _rank(row: Row) -> tuple:
    _, _, event_time, knowable_at, value, table, ref = row
    return (event_time, knowable_at, table or "", ref, float("-inf") if value is None else value)


def as_of(
    rows: Iterable[Row], entities: Iterable[str], global_entity: str, at: datetime
) -> list[tuple]:
    entities = tuple(entities)
    cutoff = micros(at)
    best: dict[tuple[str, str], Row] = {}
    for row in rows:
        if row[3] > cutoff:
            continue
        entity = row[0]
        targets = entities if entity == global_entity else (entity,) if entity in entities else ()
        for target in targets:
            current = best.get((target, row[1]))
            if current is None or _rank(row) > _rank(current):
                best[(target, row[1])] = row
    return [
        (entity, feature, row[4], row[2])
        for (entity, feature), row in sorted(best.items())
        if row[4] is not None
    ]


def inputs_hash(rows: Iterable[Row], entities, global_entity: str, at: datetime) -> str:
    stamp = at.astimezone(UTC).isoformat()
    return fingerprint(
        sorted(
            [entity, stamp, feature, float(value), iso(event_time)]
            for entity, feature, value, event_time in as_of(rows, entities, global_entity, at)
        )
    )


class Bundle:
    def __init__(self, payload: dict):
        if payload.get("format") != FORMAT:
            raise ValueError(f"not a {FORMAT} bundle: format {payload.get('format')!r}")
        self.payload = payload
        self.name = payload["name"]
        self.recorded_id = payload.get("id")
        self.at = instant(payload["at"])
        self.symbols = tuple(payload["symbols"])
        self.global_entity = payload["global_entity"]
        self.features = tuple(payload["features"])
        self.lineage = Lineage.of(payload["lineage"])
        self.recorded_set_hash = payload["set_hash"]
        self.source = dict(payload.get("source", {}))
        self.bars = tuple(
            Bar(r[0], r[1], r[2], tuple(r[3:-1]), int(r[-1])) for r in payload["bars"]
        )
        self.rows = tuple(
            (r[0], r[1], int(r[2]), int(r[3]), None if r[4] is None else float(r[4]), r[5], r[6])
            for r in payload["rows"]
        )
        found = (
            bar_items(self.bars)
            + filing_items(payload["filings"])
            + release_items(payload["releases"], self.lineage.levels, self.global_entity)
        )
        self.items = {i.id: i for i in found}
        self.days: dict[str, list[str]] = {}
        for b in sorted(self.bars, key=lambda b: b.day):
            self.days.setdefault(b.symbol, []).append(b.day)
        self._reached = self._dependents()

    @classmethod
    def read(cls, path) -> "Bundle":
        data = Path(path).read_bytes()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        return cls(json.loads(data))

    @property
    def id(self) -> str:
        return fingerprint({k: v for k, v in self.payload.items() if k != "id"})

    @property
    def set_hash(self) -> str:
        return set_hash(self.items.values())

    def by_kind(self) -> dict[str, int]:
        counts = Counter(i.kind for i in self.items.values())
        return {kind: counts.get(kind, 0) for kind in KINDS}

    def lineage_of(self, row: Row) -> tuple[str, ...]:
        found = self.lineage.owner(row)
        if row[5] != self.lineage.prices:
            return (found,)
        known = self.days.get(row[0], [])
        if row[6] not in known:
            raise ValueError(f"{row[1]} of {row[0]} for {row[6]} has no daily bar")
        end = known.index(row[6])
        start = max(0, end - self.lineage.window)
        return tuple(f"{BAR}:{row[0]}/{d}" for d in known[start : end + 1])

    def _dependents(self) -> dict[str, tuple[int, ...]]:
        found: dict[str, list[int]] = {}
        for i, row in enumerate(self.rows):
            for item in {self.lineage.owner(row), *self.lineage_of(row)}:
                found.setdefault(item, []).append(i)
        return {k: tuple(v) for k, v in found.items()}

    def check(self) -> list[str]:
        problems = []
        if self.recorded_id is not None and self.recorded_id != self.id:
            problems.append(
                f"the bundle's content hashes to {self.id[:12]}, not {self.recorded_id[:12]}"
            )
        if self.set_hash != self.recorded_set_hash:
            problems.append(
                f"the items hash to {self.set_hash[:12]}, the bundle records "
                f"{self.recorded_set_hash[:12]}"
            )
        later = [i for i in self.items.values() if i.knowable_at > micros(self.at)]
        if later:
            problems.append(f"{len(later)} items are knowable after the instant: {later[0].id}")
        problems += self._accounting(self.rows, ())
        problems += differences(self.rebuild(()), self.rows)[:SAMPLE]
        return problems

    def _accounting(self, rows: Iterable[Row], withheld: Iterable[str]) -> list[str]:
        gone, cutoff, problems = set(withheld), micros(self.at), []
        for row in rows:
            try:
                owner = self.lineage.owner(row)
            except ValueError as error:
                problems.append(str(error))
                continue
            if owner in gone:
                problems.append(f"{row[1]} of {row[0]} for {row[6]} comes from withheld {owner}")
            elif owner not in self.items:
                problems.append(
                    f"{row[1]} of {row[0]} for {row[6]} comes from {owner}, not an item"
                )
            if row[3] > cutoff:
                problems.append(f"{row[1]} of {row[0]} for {row[6]} is knowable after the instant")
        return problems[:SAMPLE]

    def rebuild(self, withheld: Iterable[str]) -> list[Row]:
        gone = set(withheld)
        kept: dict[str, list[Bar]] = {}
        for b in self.bars:
            if b.id not in gone:
                kept.setdefault(b.symbol, []).append(b)
        prices = [
            r
            for symbol in sorted(kept)
            for r in price_rows(kept[symbol], self.lineage)
            if r[1] in self.features
        ]
        others = [
            r
            for r in self.rows
            if r[5] != self.lineage.prices and self.lineage.owner(r) not in gone
        ]
        return prices + others

    def withhold(self, withheld: Iterable[str] = ()) -> World:
        withheld = tuple(sorted(set(withheld)))
        unknown = [i for i in withheld if i not in self.items]
        if unknown:
            raise ValueError(f"cannot withhold {unknown}: not items of {self.name}")
        if not withheld:
            return World(self, (), self.rows, 0, 0)
        gone = set(withheld)
        touched = {i for item in gone for i in self._reached.get(item, ())}
        needed: dict[str, set[tuple[str, str]]] = {}
        for i in touched:
            row = self.rows[i]
            if self.lineage.owner(row) not in gone:
                needed.setdefault(row[0], set()).add((row[1], row[6]))
        recomputed = []
        for symbol, pairs in sorted(needed.items()):
            bars = [b for b in self.bars if b.symbol == symbol and b.id not in gone]
            recomputed += [r for r in price_rows(bars, self.lineage) if (r[1], r[6]) in pairs]
        rows = tuple(r for i, r in enumerate(self.rows) if i not in touched) + tuple(recomputed)
        problems = self._accounting(rows, withheld)
        problems += [f"{p} (against a full rebuild)" for p in differences(rows, self.rebuild(gone))]
        return World(self, withheld, rows, len(touched), len(recomputed), tuple(problems[:SAMPLE]))

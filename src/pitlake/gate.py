from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta

from pitlake.config import validate_identifier
from pitlake.silver import BINANCE_SPOT_TRADES_SILVER, COINBASE_SPOT_CANDLES_1M_SILVER, SilverSpec

REJECTION_RATE = "rejection_rate"
VENUE_DIVERGENCE = "venue_divergence"
VENUE_MISSING = "venue_missing"
CHECKS = (REJECTION_RATE, VENUE_DIVERGENCE, VENUE_MISSING)
ANY = "*"
MINUTES_PER_DAY = 1440


class GateFailure(Exception):
    def __init__(self, breaches: list["Result"]):
        self.breaches = breaches
        detail = "; ".join(
            f"{b.check} {b.subject} {b.day}: {b.failed} of {b.measured} (allowed {b.allowed:g})"
            for b in breaches[:20]
        )
        more = f" and {len(breaches) - 20} more" if len(breaches) > 20 else ""
        super().__init__(f"quality gate failed on {len(breaches)} checks: {detail}{more}")


@dataclass(frozen=True)
class Threshold:
    check: str
    subject: str
    max_rate: float
    tolerance_rows: int
    description: str

    def __post_init__(self) -> None:
        if self.check not in CHECKS:
            raise ValueError(f"{self.check!r}: check must be one of {CHECKS}")
        if not 0 <= self.max_rate <= 1 or self.tolerance_rows < 0:
            raise ValueError(f"{self.check} {self.subject}: invalid limits")
        if not self.description.strip():
            raise ValueError(f"{self.check} {self.subject}: every threshold must say why")

    def allowed(self, measured: int) -> float:
        return self.max_rate * measured + self.tolerance_rows


@dataclass(frozen=True)
class VenuePair:
    name: str
    left: SilverSpec
    left_symbol: str
    right: SilverSpec
    right_symbol: str
    max_close_diff_bps: float
    grace_days: int = 2

    def __post_init__(self) -> None:
        validate_identifier(self.name.lower())


BTC = VenuePair(
    name="BTC",
    left=BINANCE_SPOT_TRADES_SILVER,
    left_symbol="BTCUSDT",
    right=COINBASE_SPOT_CANDLES_1M_SILVER,
    right_symbol="BTC-USD",
    max_close_diff_bps=100,
)
PAIRS = (BTC,)

THRESHOLDS = (
    Threshold(
        REJECTION_RATE,
        ANY,
        0.01,
        2,
        "Any one rule may reject at most 1% of a day's rows, plus 2 rows so that one bad row in a "
        "small file (4 daily bars) does not stop the platform.",
    ),
    Threshold(
        REJECTION_RATE,
        "edgar_company_facts.duplicate_key",
        1,
        0,
        "Every EDGAR snapshot repeats the facts of the snapshots before it (ADR 0004).",
    ),
    Threshold(
        REJECTION_RATE,
        "edgar_filings.duplicate_key",
        1,
        0,
        "Every EDGAR snapshot repeats the filings of the snapshots before it (ADR 0004).",
    ),
    Threshold(
        VENUE_DIVERGENCE,
        ANY,
        0.001,
        3,
        "At most 0.1% of a day's minutes, plus 3, may have closes more than max_close_diff_bps "
        "apart. Over 2025-01-01 to 2026-10-06 only the crash of 2025-10-10 exceeded it.",
    ),
    Threshold(
        VENUE_MISSING,
        ANY,
        0.01,
        0,
        "At most 1% of a day's minutes may be on one venue only. Data missing on one venue and "
        "present on the other is an ingestion fault, not a market event (spec section 2).",
    ),
)


@dataclass(frozen=True)
class Measurement:
    check: str
    subject: str
    day: date
    measured: int
    failed: int


@dataclass(frozen=True)
class Result:
    check: str
    subject: str
    day: date
    measured: int
    failed: int
    allowed: float
    passed: bool
    waived: bool
    waiver_reason: str | None

    @property
    def breach(self) -> bool:
        return not self.passed and not self.waived


def threshold_for(check: str, subject: str, thresholds: Iterable[Threshold]) -> Threshold:
    candidates = [t for t in thresholds if t.check == check]
    for t in candidates:
        if t.subject == subject:
            return t
    for t in candidates:
        if t.subject == ANY:
            return t
    raise ValueError(f"no threshold for {check} {subject}")


def judge(
    measurements: Iterable[Measurement],
    thresholds: Iterable[Threshold],
    waivers: Mapping[tuple[str, str, date], str],
) -> list[Result]:
    thresholds = tuple(thresholds)
    results = []
    for m in measurements:
        allowed = threshold_for(m.check, m.subject, thresholds).allowed(m.measured)
        passed = m.failed <= allowed
        reason = None if passed else waivers.get((m.check, m.subject, m.day))
        results.append(
            Result(
                m.check,
                m.subject,
                m.day,
                m.measured,
                m.failed,
                allowed,
                passed,
                reason is not None,
                reason,
            )
        )
    return results


def rejection_measurements(rows: Iterable[tuple]) -> list[Measurement]:
    judged: dict[tuple[str, date], int] = {}
    rejected: dict[tuple[str, date, str], int] = {}
    for table, day, rows_in, by_rule in rows:
        judged[(table, day)] = judged.get((table, day), 0) + rows_in
        for rule, n in (by_rule or {}).items():
            rejected[(table, day, rule)] = rejected.get((table, day, rule), 0) + n
    return [
        Measurement(REJECTION_RATE, f"{table}.{rule}", day, judged[(table, day)], n)
        for (table, day, rule), n in sorted(rejected.items())
    ]


def venue_measurements(pair: VenuePair, days: Iterable[tuple]) -> list[Measurement]:
    result = []
    for day, minutes, divergent, missing in days:
        result.append(Measurement(VENUE_DIVERGENCE, pair.name, day, minutes, divergent))
        result.append(Measurement(VENUE_MISSING, pair.name, day, minutes, missing))
    return result


@dataclass(frozen=True)
class DayFiles:
    day: date
    left_file: str | None
    left_sha256: str | None
    right_file: str | None
    right_sha256: str | None


def days_to_compare(
    left: Mapping[date, tuple[str, str]],
    right: Mapping[date, tuple[str, str]],
    compared: set[tuple[date, str | None, str | None]],
    today: date,
    grace_days: int,
    period: tuple[date, date] | None = None,
) -> list[DayFiles]:
    # A day on one venue only waits grace_days for the other venue to publish.
    result = []
    for day in sorted(set(left) | set(right)):
        if period is not None and not period[0] <= day <= period[1]:
            continue
        if (day not in left or day not in right) and day >= today - timedelta(days=grace_days):
            continue
        lf, ls = left.get(day, (None, None))
        rf, rs = right.get(day, (None, None))
        if period is None and (day, ls, rs) in compared:
            continue
        result.append(DayFiles(day, lf, ls, rf, rs))
    return result

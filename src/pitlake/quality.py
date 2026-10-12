import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

DROP = "drop"
HALT = "halt"
RECORD = "record"
ACTIONS = (DROP, HALT, RECORD)

NO_FAULT = "none"
KNOWABLE_AT_MISSING = "knowable_at_missing"
KNOWABLE_AT_BEFORE_EVENT = "knowable_at_before_event"
FAULTS = (NO_FAULT, KNOWABLE_AT_MISSING, KNOWABLE_AT_BEFORE_EVENT)

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class QualityHalt(Exception):
    def __init__(self, table: str, violations: Mapping[str, int]):
        self.table = table
        self.violations = dict(violations)
        detail = ", ".join(f"{rule}: {n} rows" for rule, n in sorted(self.violations.items()))
        super().__init__(f"{table}: halt rules failed ({detail}); nothing was written")


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str
    action: str
    description: str

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError(f"invalid rule name: {self.name!r}")
        if self.action not in ACTIONS:
            raise ValueError(f"{self.name}: action must be one of {ACTIONS}")
        if not self.condition.strip():
            raise ValueError(f"{self.name}: empty condition")
        if not self.description.strip():
            raise ValueError(f"{self.name}: every rule must explain what a failure means")


RECORD_PARSES = Rule(
    "record_parses",
    "_corrupt_record IS NULL",
    DROP,
    "Bronze could not split the raw record into columns (a malformed line or a record of the wrong "
    "shape). The raw text is kept in the quarantine record.",
)
SOURCE_REGISTERED = Rule(
    "source_registered",
    "_registered",
    HALT,
    "The row's source and dataset are not in control.source_registry, so nobody has reviewed it. "
    "Unreviewed data must not enter Silver.",
)
TIMESTAMP_PLAUSIBLE = "timestamp_plausible"
KNOWABLE_AT_PRESENT = Rule(
    "knowable_at_present",
    "knowable_at IS NOT NULL",
    HALT,
    "The row has no knowable_at. A missing value would silently disable point-in-time protection.",
)
KNOWABLE_AT_NOT_BEFORE_EVENT = Rule(
    "knowable_at_not_before_event",
    "knowable_at >= event_time",
    HALT,
    "knowable_at is earlier than the event it describes, which means it was mis-assigned.",
)
KEY_PRESENT = "key_present"
DUPLICATE_KEY = "duplicate_key"
SEQUENCE_CONTINUOUS = "sequence_continuous"

_BEFORE_KNOWABLE = (RECORD_PARSES.name, SOURCE_REGISTERED.name, TIMESTAMP_PLAUSIBLE)


def timestamp_plausible(condition: str, description: str) -> Rule:
    return Rule(TIMESTAMP_PLAUSIBLE, condition, DROP, description)


def key_present(columns: Sequence[str]) -> Rule:
    if not columns:
        raise ValueError("key_present needs at least one column")
    return Rule(
        KEY_PRESENT,
        " AND ".join(f"`{c}` IS NOT NULL" for c in columns),
        DROP,
        f"A column of the record's key ({', '.join(columns)}) is missing or does not parse, so the "
        "record cannot be told apart from others.",
    )


def duplicate_key(key: Sequence[str], scope: str) -> Rule:
    across = "the files of the same symbol" if scope == "symbol" else "the same file"
    return Rule(
        DUPLICATE_KEY,
        f"first row per ({', '.join(key)}) within {across}",
        DROP,
        f"Another row with the same key ({', '.join(key)}) within {across} is already in Silver.",
    )


def sequence_continuous(value: str, step: int, description: str) -> Rule:
    return Rule(SEQUENCE_CONTINUOUS, f"{value} advances by {step}", RECORD, description)


def first_failure_sql(rules: Sequence[Rule]) -> str:
    judged = [r for r in rules if r.action != RECORD and r.name != DUPLICATE_KEY]
    if not judged:
        raise ValueError("no rules to judge")
    # A NULL condition counts as a failure.
    branches = " ".join(
        f"WHEN NOT coalesce(({r.condition}), false) THEN '{r.name}'" for r in judged
    )
    return f"CASE {branches} ELSE NULL END"


def passes_sql(rules: Sequence[Rule], names: Sequence[str]) -> str:
    chosen = [r for r in rules if r.name in names]
    if not chosen:
        raise ValueError(f"none of {list(names)} are among the rules")
    return " AND ".join(f"coalesce(({r.condition}), false)" for r in chosen)


def fault_target_sql(rules: Sequence[Rule]) -> str:
    return passes_sql(rules, _BEFORE_KNOWABLE)


def halt_violations(rules: Sequence[Rule], counts: Mapping[str | None, int]) -> dict[str, int]:
    return {r.name: counts[r.name] for r in rules if r.action == HALT and counts.get(r.name, 0) > 0}


def validate_rules(rules: Sequence[Rule]) -> None:
    names = [r.name for r in rules]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"rule names must be unique, repeated: {duplicates}")

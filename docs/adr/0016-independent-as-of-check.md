# ADR 0016: An independent implementation checks what a backtest saw

Status: proposed. Builds on [ADR 0008](0008-gold-and-point-in-time-access.md) and
[ADR 0009](0009-research-and-reproducible-backtests.md).

## Context

Every backtest records `input_hash`, the SHA-256 of every value it could see: per asset and
06:00 UTC decision, each feature's value and event time as `pitlake.pit.as_of` returned them from
the Gold version it read (ADR 0009). The replay proves that the same code returns the same inputs.
It cannot prove that the code applies the point-in-time rule correctly, because a mistake in
`as_of` would be repeated by the replay. The leakage audit (ADR 0008) checks Gold itself, not what
a backtest read from it.

## Decisions

**`tools/pitcheck` rebuilds a backtest's inputs from the raw Gold rows with a separate
implementation of the rule, and compares the hash.** `make pitcheck TARGET=<target>
BACKTEST=<id>` runs two steps:

1. `scripts/export_backtest_inputs.py` reads the backtest's record from `control.backtest_runs` and
   the rows of the four features it uses from `gold.features VERSION AS OF` the recorded version,
   through the SQL warehouse, into `data/pitcheck/<id>/`. It only reads.
2. The OCaml program applies the rule to those rows for every asset and decision of the recorded
   period: among the rows with `knowable_at` at or before the decision, the latest `event_time`,
   then the latest revision of it; a macro row (entity `US`) applies to every asset; a value that
   is NULL in the chosen row is not seen. It serializes the result exactly as Python's
   `json.dumps` does, with the same shortest float digits and ISO time stamps, hashes it with its
   own SHA-256, and prints IDENTICAL or DIFFERENT, exiting non-zero on a difference.

It shares no code with `pitlake.pit` or the research job: it uses a different language and does
not use Spark. It also has its own SHA-256 and calendar arithmetic, and depends only on the OCaml
standard library, as `doccheck` does (ADR 0015). Its tests use the published SHA-256 test vectors,
and Python's output for the float and time formats.

## Result (prod, 2026-10-26)

Backtest `a4d4c4e0955046b8a6fb6a171f8b1071` (one of the 24 of research run 238695896043288,
2025-02-01 to 2026-10-08, `gold.features` version 4): 35,923 Gold rows, 11,058 values rebuilt at
615 decisions, and the rebuilt hash `7d0c6270…848d` **equals the recorded one**. All 48 prod
backtest records, the originals and their replays, carry that hash.

The same check with the quarterly net income made knowable at its period end instead of its
filing, which is the leak point-in-time access exists to prevent, gave `b8de9335…e5ed` and
DIFFERENT.

## Consequences

- A change to how research picks its inputs (features, universe, decision hour, the as-of rule)
  must be made in both implementations, or `pitcheck` reports DIFFERENT for new backtests.
  Old backtests keep matching, since their record names everything the check needs.
- `make pitcheck` needs the OCaml toolchain (ADR 0015) and a login to the workspace; it is a
  verification step, not part of the jobs.

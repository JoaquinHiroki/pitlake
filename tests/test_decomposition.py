import json
import math
import re
import time
import tomllib
from datetime import UTC, date, datetime, timedelta

import pytest
import yaml

from pitlake.backtest import GRID
from pitlake.decomposition import (
    ACROSS,
    AFTER,
    BEFORE,
    BLIND,
    INFERRED,
    MEMORISED,
    PRIOR,
    PROBE_SYSTEM,
    PROBES,
    RECALL,
    RECALL_GAIN,
    STRADDLES,
    TOOL_GAIN,
    TOOLED,
    TOOLS_OVER_MEMORY,
    Question,
    Scored,
    condition_config,
    decompose,
    era,
    estimate,
    parse_price,
    questions,
    recall,
    recall_error,
    run_untooled,
    text_of,
    untooled_prompts,
)
from pitlake.harness import SYMBOLS, agent_config
from pitlake.jobs.decompose import (
    DECOMPOSED,
    FAILED,
    PARTIAL,
    Decider,
    decision_key,
    decompose_instants,
    monthly,
    parse_args,
    probe_key,
    report,
    stored_test,
)
from pitlake.jobs.suffice import Late
from pitlake.tables import (
    DECISION_SAMPLES,
    DECOMPOSITION_SCORES,
    DECOMPOSITION_SCORES_SCHEMA,
    DECOMPOSITION_TESTS,
    DECOMPOSITION_TESTS_SCHEMA,
    DECOMPOSITIONS,
    DECOMPOSITIONS_SCHEMA,
    RECALL_PROBES,
    RECALL_PROBES_SCHEMA,
)

START = date(2024, 2, 1)
END = date(2024, 8, 1)
CUTOFF = date(2024, 4, 15)
RECORD = {
    "backtest_id": "b",
    "research_run_id": "r",
    "period_start": START,
    "period_end": END,
    "input_hash": "recorded",
    "params": GRID[0].to_json(),
}
VERSIONS = {"gold.features": 4}


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


class Collect:
    def __init__(self):
        self.rows: dict[str, list[tuple]] = {}

    def add(self, table: str, rows) -> None:
        self.rows.setdefault(table, []).extend(rows)

    def flush(self, force: bool = True) -> None:
        pass

    def named(self, table: str, schema: str) -> list[dict]:
        return [dict(zip(columns(schema), r, strict=True)) for r in self.rows.get(table, [])]


def price_on(symbol: str, day: date) -> float:
    step = {"AAPL": 1, "BTCUSDT": -1, "MSFT": 0, "NVDA": 2, "SPY": 0}[symbol]
    return 100.0 + step * (day - START).days / 10


def closes(until: date = date(2024, 8, 20)) -> list[tuple]:
    rows, day = [], START - timedelta(days=30)
    while day <= until:
        event = datetime(day.year, day.month, day.day, 21, tzinfo=UTC)
        for symbol in SYMBOLS:
            rows.append(
                (
                    symbol,
                    "close",
                    event,
                    event + timedelta(hours=1),
                    price_on(symbol, day),
                    "bars_1d",
                    f"{symbol}/{day}",
                )  # fmt: skip
            )
        day += timedelta(days=1)
    return rows


RATINGS = {"AAPL": "overweight", "BTCUSDT": "underweight", "MSFT": "neutral", "NVDA": "neutral",
           "SPY": "neutral"}  # fmt: skip


class Model:
    def __init__(self, fail: bool = False):
        self.calls: list[tuple[str, bool]] = []
        self.fail = fail

    def __call__(self, messages: list[dict], tools: list[dict] | None) -> dict:
        system = messages[0]["content"]
        if system == PROBE_SYSTEM:
            self.calls.append(("probe", False))
            day = date.fromisoformat(re.search(r"\d{4}-\d{2}-\d{2}", messages[1]["content"])[0])
            name = re.search(r"\((\w+)\)", messages[1]["content"])[1]
            symbol = "BTCUSDT" if name == "BTC" else name
            known = price_on(symbol, day) if day < CUTOFF else 5.0
            return {"content": f"{known:.2f}"}
        if self.fail:
            raise RuntimeError("endpoint down")
        if tools is not None:
            self.calls.append((TOOLED, True))
            results = [m for m in messages if m["role"] == "tool"]
            if not results:
                call = {"name": "symbol_snapshot", "arguments": json.dumps({"symbol": "AAPL"})}
                return {"content": "", "tool_calls": [{"id": "1", "function": call}]}
            return {"content": json.dumps(RATINGS)}
        if "instant" in system:
            self.calls.append((RECALL, False))
            when = re.search(r"\d{4}-\d{2}-\d{2}", system)[0]
            knows = date.fromisoformat(when) < CUTOFF
            return {"content": json.dumps(RATINGS if knows else {s: "neutral" for s in SYMBOLS})}
        self.calls.append((BLIND, False))
        return {"content": json.dumps({s: "neutral" for s in SYMBOLS})}


def args_of(*extra: str):
    return parse_args(
        [
            "--catalog",
            "c",
            "--endpoint",
            "e",
            "--cutoff",
            CUTOFF.isoformat(),
            "--samples",
            "2",
            "--run-id",
            "run",
            *extra,
        ]  # fmt: skip
    )


def run(args, model, tests=None, probes=None, deadline=None, rows=None):
    writer = Collect()
    rows = closes() if rows is None else rows
    found = decompose_instants(
        RECORD,
        rows,
        VERSIONS,
        rows,
        model,
        tests or {},
        probes or {},
        args,
        writer,
        time.monotonic() + 600 if deadline is None else deadline,
        datetime(2026, 11, 13, tzinfo=UTC),
    )
    return found, writer


def test_the_untooled_conditions_differ_from_the_tooled_one_only_in_tools_and_date():
    at = datetime(2024, 3, 1, 6, tzinfo=UTC)
    blind, dated = untooled_prompts(BLIND, at), untooled_prompts(RECALL, at)
    assert "2024" not in blind[0]["content"] and "2024-03-01 06:00" in dated[0]["content"]
    assert blind[1] == dated[1] and "no tools" in blind[0]["content"]
    assert condition_config(TOOLED, "e") == agent_config("e")
    assert condition_config(BLIND, "e") != condition_config(RECALL, "e")
    seen = []

    def complete(messages, tools):
        seen.append(tools)
        return {"content": [{"type": "reasoning"}, {"type": "text", "text": json.dumps(RATINGS)}]}

    sample = run_untooled(complete, RECALL, at)
    assert seen == [None] and json.loads(sample.decision) == RATINGS and sample.model_calls == 1
    bad = run_untooled(lambda m, t: {"content": "I cannot rate"}, BLIND, at)
    assert bad.decision is None and "no JSON" in bad.error and bad.model_calls == 1
    assert text_of(None) == "" and text_of("x") == "x"


def test_an_instant_is_before_the_cutoff_only_when_its_whole_window_is():
    cutoff = date(2024, 9, 1)
    assert era(datetime(2024, 8, 1, 6, tzinfo=UTC), 30, cutoff) == BEFORE
    assert era(datetime(2024, 8, 2, 6, tzinfo=UTC), 30, cutoff) == STRADDLES
    assert era(datetime(2024, 9, 1, 6, tzinfo=UTC), 30, cutoff) == AFTER


def test_a_remembered_close_and_its_error():
    assert parse_price("$1,234.56") == 1234.56
    assert parse_price("On 2024-03-28 NVDA closed at 903.56.") == 903.56
    assert parse_price("I'm sorry, but I don't have that information.") is None
    assert parse_price(None) is None and parse_price("0") is None
    assert estimate(["100", "no idea", "110", "90"]) == 100.0 and estimate([None]) is None
    assert recall_error(110.0, 100.0) == pytest.approx(math.log(1.1))
    assert recall_error(None, 100.0) is None


def test_the_closes_asked_about_and_the_median_error():
    rows = closes()
    at = datetime(2024, 3, 1, 6, tzinfo=UTC)
    asked = questions(rows, [at, at])
    (mine,) = asked.values()
    assert [q.symbol for q in mine] == list(SYMBOLS)
    assert all(q.session == date(2024, 2, 29) for q in mine)
    estimates = {q.key: q.actual for q in mine}
    estimates[mine[0].key], estimates[mine[1].key] = None, mine[1].actual * 2
    (found,) = recall(asked, estimates, CUTOFF)
    assert found.era == BEFORE and found.error == 0.0 and found.recalled(0.05)
    estimates[mine[2].key] = mine[2].actual * 2
    (worse,) = recall(asked, estimates, date(2024, 2, 1))
    assert worse.era == AFTER and worse.error == pytest.approx(math.log(2))


def scored(where: str, at: int, blind: float, memory: float, tools: float) -> Scored:
    return Scored(datetime(2024, 1, at, tzinfo=UTC), where, blind, blind + memory,
                  blind + memory + tools)  # fmt: skip


def test_the_decomposition_and_its_intervals():
    found = [
        scored(BEFORE, 1, 0.00, 0.03, 0.00),
        scored(BEFORE, 2, 0.01, 0.02, 0.01),
        scored(BEFORE, 3, -0.01, 0.04, -0.01),
        scored(AFTER, 4, 0.00, 0.00, 0.02),
        scored(AFTER, 5, 0.01, 0.01, 0.01),
        scored(AFTER, 6, -0.01, -0.01, 0.03),
    ]
    by_key = {(e.component, e.era): e for e in decompose(found, rounds=2000)}
    assert by_key[PRIOR, BEFORE].value == pytest.approx(0.0)
    assert by_key[RECALL_GAIN, BEFORE].value == pytest.approx(0.03)
    assert by_key[RECALL_GAIN, AFTER].value == pytest.approx(0.0)
    assert by_key[MEMORISED, ACROSS].value == pytest.approx(0.03)
    assert by_key[INFERRED, AFTER].value == pytest.approx(0.02)
    assert by_key[TOOL_GAIN, AFTER].value == by_key[INFERRED, AFTER].value
    assert by_key[TOOLS_OVER_MEMORY, ACROSS].value == pytest.approx(-0.02)
    memorised = by_key[MEMORISED, ACROSS]
    assert memorised.instants == 6 and memorised.low < 0.03 < memorised.high
    assert memorised.low > 0
    again = {(e.component, e.era): e for e in decompose(found, rounds=2000)}
    assert again == by_key
    alone = {(e.component, e.era): e for e in decompose(found[:1] + found[3:], rounds=200)}
    assert alone[MEMORISED, ACROSS].value is not None and not alone[MEMORISED, ACROSS].interval
    assert alone[INFERRED, AFTER].interval and not alone[PRIOR, BEFORE].interval
    after = {(e.component, e.era): e for e in decompose(found[3:], rounds=200)}
    assert after[MEMORISED, ACROSS].value is None and after[PRIOR, BEFORE].value is None


def test_arguments_and_the_monthly_instants():
    args = args_of("--at", "2024-03-01, 2024-02-01 2024-03-01")
    assert args.at == (date(2024, 2, 1), date(2024, 3, 1)) and args.cutoff == CUTOFF
    assert args.samples == 2 and args.days == 30 and args.tolerance == 0.05
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c", "--endpoint", "e"])
    with pytest.raises(SystemExit):
        parse_args(["--catalog", "c", "--endpoint", " ", "--cutoff", "2024-01-01"])
    assert monthly(date(2024, 11, 2), date(2025, 2, 1)) == (
        date(2024, 12, 1), date(2025, 1, 1), date(2025, 2, 1),
    )  # fmt: skip
    assert monthly(date(2025, 1, 2), date(2025, 1, 15)) == ()


def test_blind_is_decided_once_and_every_test_is_reused_by_its_key():
    args = args_of()
    first, second = (datetime(2024, m, 1, 6, tzinfo=UTC) for m in (2, 3))
    assert decision_key(BLIND, "e", first, VERSIONS, 2) == decision_key(BLIND, "e", second, {}, 2)
    assert decision_key(RECALL, "e", first, VERSIONS, 2) != decision_key(
        RECALL, "e", second, VERSIONS, 2
    )
    assert decision_key(TOOLED, "e", first, VERSIONS, 2) != decision_key(TOOLED, "e", first, {}, 2)
    model, writer = Model(), Collect()
    decider = Decider(model, closes(), VERSIONS, args, writer, time.monotonic() + 60, first, {})
    blind = decider(BLIND, first)
    assert decider(BLIND, second) is blind and blind.at is None
    tooled = decider(TOOLED, first)
    assert tooled.valid and tooled.samples == 2 and tooled.input_hash != blind.input_hash
    assert len(writer.rows[DECISION_SAMPLES]) == 4 and decider.decided == 2
    late = Decider(model, closes(), VERSIONS, args, Collect(), time.monotonic() - 1, first,
                   decider.tests)  # fmt: skip
    assert late(TOOLED, first) == tooled
    with pytest.raises(Late):
        late(RECALL, first)
    broken = Decider(Model(fail=True), closes(), VERSIONS, args, Collect(), time.monotonic() + 60,
                     first, {})  # fmt: skip
    failed = broken(RECALL, first)
    assert not failed.valid and failed.errors == 2 and broken.tests == {}


def test_every_instant_is_decomposed_and_recorded():
    args = args_of()
    model = Model()
    found, writer = run(args, model)
    assert [i.day for i in found.instants] == [date(2024, m, 1) for m in (2, 3, 5, 6, 7)]
    assert found.straddling == (date(2024, 4, 1),)
    assert [d for d, _ in found.left_out] == [date(2024, 8, 1)]
    assert all(i.state == DECOMPOSED for i in found.instants)
    assert [i.era for i in found.instants] == [BEFORE, BEFORE, AFTER, AFTER, AFTER]
    assert sum(1 for c, _ in model.calls if c == BLIND) == 2
    tests = writer.named(DECOMPOSITION_TESTS, DECOMPOSITION_TESTS_SCHEMA)
    assert len(tests) == 1 + 2 * 5 and all(t["valid"] for t in tests)
    assert {t["condition"] for t in tests if t["decision_at"] is None} == {BLIND}
    probes = writer.named(RECALL_PROBES, RECALL_PROBES_SCHEMA)
    assert len(probes) == len({(p["symbol"], p["session_date"]) for p in probes})
    assert all(len(p["answers"]) == PROBES for p in probes)
    early = [p for p in probes if p["session_date"] < CUTOFF]
    assert early and all(p["error"] == pytest.approx(0.0) for p in early)
    scores = writer.named(DECOMPOSITION_SCORES, DECOMPOSITION_SCORES_SCHEMA)
    assert [s["era"] for s in scores] == [BEFORE, BEFORE, AFTER, AFTER, AFTER]
    assert all(s["blind"] == 0.0 for s in scores)
    assert all(s["recall"] > 0 for s in scores[:2]) and all(s["recall"] == 0 for s in scores[2:])
    assert all(s["tooled"] > 0 and s["recall_error"] is not None for s in scores)
    rows = writer.named(DECOMPOSITIONS, DECOMPOSITIONS_SCHEMA)
    by_key = {(r["component"], r["era"]): r for r in rows}
    assert by_key[MEMORISED, ACROSS]["estimate"] > 0 and by_key[MEMORISED, ACROSS]["low"] > 0
    assert by_key[INFERRED, AFTER]["estimate"] > 0 and by_key[INFERRED, AFTER]["level"] == 0.95
    lines = report(args, found)
    text = "\n".join(lines)
    assert lines[0].startswith("RECALL: e asked with no tools")
    assert "before the cutoff: 4 of 4 times recalled" in text
    assert "after the cutoff: 0 of 5 times recalled" in text
    assert "LEFT OUT: 2024-04-01: its 30-day window straddles the cutoff" in text
    assert "LEFT OUT: 2024-08-01: the outcome is not known yet" in text
    assert "DECOMPOSED: 2024-02-01 06:00 UTC, before the cutoff; outcome to" in text
    assert "quality: blind +0.0000, recall +" in text
    assert (
        "DECOMPOSITION: e, cutoff 2024-04-15, 2 instants before it and 3 after (1 straddling"
        in text
    )
    assert "  memorised: +" in text and "  inferred: +" in text


def test_a_later_run_reuses_everything_and_a_late_one_stops():
    args = args_of()
    first, writer = run(args, Model())
    tests = {
        t["test_key"]: stored_test(t)
        for t in writer.named(DECOMPOSITION_TESTS, DECOMPOSITION_TESTS_SCHEMA)
    }
    probes = {
        probe_key("e", p["symbol"], p["session_date"]): p["estimate"]
        for p in writer.named(RECALL_PROBES, RECALL_PROBES_SCHEMA)
    }
    model = Model()
    again, rewritten = run(args, model, tests, probes)
    assert model.calls == [] and DECOMPOSITION_TESTS not in rewritten.rows
    assert RECALL_PROBES not in rewritten.rows
    assert [e.value for e in again.estimates] == [e.value for e in first.estimates]
    some = dict(list(tests.items())[:3])
    late, nothing = run(args, Model(), some, probes, deadline=time.monotonic() - 1)
    assert {i.state for i in late.instants} == {DECOMPOSED, PARTIAL} and late.estimates == []
    assert DECOMPOSITIONS not in nothing.rows and DECOMPOSITION_TESTS not in nothing.rows
    assert "stopped with" in "\n".join(report(args, late))


def test_a_named_instant_with_no_outcome_fails_and_an_endpoint_failure_is_partial():
    args = args_of("--at", "2024-02-01 2024-08-01")
    found, writer = run(args, Model())
    assert [i.state for i in found.instants] == [DECOMPOSED, FAILED]
    assert "the outcome is not known yet" in found.instants[1].unknown
    assert found.estimates == [] and DECOMPOSITIONS not in writer.rows
    broken, _ = run(args_of("--at", "2024-02-01"), Model(fail=True))
    (only,) = broken.instants
    assert only.state == PARTIAL and only.failed == 3
    assert "the endpoint failed in 3 of them" in "\n".join(report(args, broken))


def test_records_match_their_tables():
    args = args_of("--at", "2024-02-01 2024-06-01")
    _, writer = run(args, Model())
    for table, schema in (
        (DECOMPOSITION_TESTS, DECOMPOSITION_TESTS_SCHEMA),
        (RECALL_PROBES, RECALL_PROBES_SCHEMA),
        (DECOMPOSITION_SCORES, DECOMPOSITION_SCORES_SCHEMA),
        (DECOMPOSITIONS, DECOMPOSITIONS_SCHEMA),
    ):
        assert writer.rows[table] and all(
            len(r) == len(columns(schema)) for r in writer.rows[table]
        )
    probes = writer.named(RECALL_PROBES, RECALL_PROBES_SCHEMA)
    assert all(isinstance(p["session_date"], date) and p["endpoint"] == "e" for p in probes)
    assert {Question(p["symbol"], p["session_date"], p["actual"]).key for p in probes}


def test_the_job_runs_the_decompose_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["decomposition"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "decompose" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.decompose:main"
    defaults = {p["name"]: p["default"] for p in job["parameters"]}
    assert set(defaults) == {
        "endpoint", "cutoff", "backtest", "at", "samples", "days", "tolerance", "minutes",
    }  # fmt: skip
    assert defaults["endpoint"] == "${var.chat_endpoint}"
    args = parse_args(["--catalog", "c", "--endpoint", "e", "--cutoff", "2024-09-01"])
    assert args.days == int(defaults["days"]) and args.samples == int(defaults["samples"])
    assert job["timeout_seconds"] > 60 * float(defaults["minutes"])

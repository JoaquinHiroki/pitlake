import json
import re
import tomllib
from datetime import UTC, date, datetime, timedelta

import pytest
import yaml

from pitlake.backtest import GRID, decision_times
from pitlake.harness import (
    AGENT_REACH,
    MAX_STEPS,
    STANCES,
    SYMBOLS,
    TOOLS,
    Sample,
    View,
    agent_config,
    agent_tools,
    canonical,
    in_reach,
    inputs_at,
    inputs_hash,
    mode,
    parse_decision,
    run_agent,
    strategy_config,
    strategy_sample,
    summarize,
)
from pitlake.information import FILING_SOURCE, PRICE_SOURCE, RELEASE_SOURCE
from pitlake.jobs.decide import (
    Decided,
    Spec,
    instant,
    parse_args,
    point_id,
    point_row,
    replay_query,
    sample_rows,
)
from pitlake.tables import DECISION_POINTS_SCHEMA, DECISION_SAMPLES_SCHEMA

FIRST = date(2025, 1, 1)
AT = datetime(2025, 2, 3, 6, tzinfo=UTC)


def day(i: int) -> datetime:
    return datetime.combine(FIRST + timedelta(days=i), datetime.min.time(), UTC)


def close(entity: str, i: int, value: float) -> tuple:
    return (entity, "close", day(i), day(i + 1), value, PRICE_SOURCE, f"{day(i):%Y-%m-%d}")


ROWS = [
    *(close("AAPL", i, 100.0 + i) for i in range(40)),
    ("AAPL", "volatility_20d", day(30), day(31), 0.02, PRICE_SOURCE, f"{day(30):%Y-%m-%d}"),
    ("AAPL", "net_income_q", day(-40), day(25), 3.6e10, FILING_SOURCE, "a-10q"),
    ("AAPL", "net_income_q", day(-40), day(28), 3.7e10, FILING_SOURCE, "a-10qa"),
    ("AAPL", "net_income_q", day(-20), day(60), 9.9e10, FILING_SOURCE, "future"),
    ("US", "cpi_yoy", day(-31), day(14), 0.029, RELEASE_SOURCE, "2025-01-15"),
    ("US", "unrate", day(-31), day(9), 4.1, RELEASE_SOURCE, "2025-01-10"),
]


def test_the_pure_python_as_of_follows_the_rule():
    times = [day(26), day(29), day(70)]
    seen = {(r[0], r[1], r[2]): r for r in inputs_at(ROWS, times)}
    assert seen[("AAPL", day(26), "net_income_q")][3] == 3.6e10
    assert seen[("AAPL", day(29), "net_income_q")][3] == 3.7e10
    assert seen[("AAPL", day(70), "net_income_q")][6] == "future"
    assert seen[("AAPL", day(29), "close")][4] == day(28)
    assert seen[("MSFT", day(29), "cpi_yoy")][3] == 0.029
    assert ("AAPL", day(29), "unrate") not in seen
    assert inputs_hash(inputs_at(ROWS, times)) == inputs_hash(
        inputs_at(list(reversed(ROWS)), times)
    )


def test_the_view_only_holds_what_was_public():
    view = View(ROWS, AT)
    assert view.latest("AAPL", "close")[4] == 132.0
    assert view.latest("AAPL", "net_income_q")[6] == "a-10qa"
    tools = agent_tools(view)
    snapshot = tools["symbol_snapshot"]({"symbol": "AAPL"})
    assert "last close 132.00 on 2025-02-02" in snapshot
    assert "Net income, quarter ending 2024-11-22: $37,000M" in snapshot
    assert "99,000" not in snapshot and "20-day volatility of daily returns 2.00%" in snapshot
    assert "Change over 1 session +0.76%, 5 sessions" in snapshot
    history = tools["price_history"]({"symbol": "AAPL", "days": 2})
    assert history.startswith("AAPL closes public at the instant, 2025-01-29 to 2025-02-02")
    assert tools["price_history"]({"symbol": "AAPL", "days": 7}).count(";") == 6
    assert "CPI inflation, year over year for 2024-12: 2.90%" in tools["macro"]({})
    assert tools["symbol_snapshot"]({"symbol": "NVDA"}).startswith("No close of NVDA")
    with pytest.raises(ValueError, match="No symbol"):
        tools["symbol_snapshot"]({"symbol": "TSLA"})


def test_no_tool_takes_a_date():
    for tool in TOOLS:
        properties = tool["function"]["parameters"]["properties"]
        assert not any(re.search("date|time|day$|as_of", p) for p in properties)


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("I think they are fine.", "no JSON object"),
        ('{"AAPL": "overweight"}', "BTCUSDT is rated None"),
        (json.dumps({**{s: "neutral" for s in SYMBOLS}, "AAPL": "buy"}), "AAPL is rated 'buy'"),
        ("[1, 2]", "no JSON object"),
        ('{"TSLA": "neutral"}', "outside the task"),
        ("{nope}", "does not parse"),
    ],
)
def test_an_answer_outside_the_format_is_invalid(text, error):
    with pytest.raises(ValueError, match=error):
        parse_decision(text)


def test_a_fenced_answer_parses_case_insensitively():
    stances = {s: STANCES[i % 3].upper() for i, s in enumerate(SYMBOLS)}
    found = parse_decision(f"Here it is:\n```json\n{json.dumps(stances)}\n```")
    assert found == {s: v.lower() for s, v in stances.items()}


def call(name, arguments, id_="c1"):
    return {"id": id_, "type": "function", "function": {"name": name, "arguments": arguments}}


class Model:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.seen = []

    def __call__(self, messages, tools):
        self.seen.append(([dict(m) for m in messages], tools))
        return self.replies.pop(0)


ANSWER = json.dumps({s: "neutral" for s in SYMBOLS})


def test_the_agent_reads_tools_then_decides():
    model = Model(
        {
            "content": None,
            "tool_calls": [
                call("symbol_snapshot", json.dumps({"symbol": "AAPL"})),
                call("nothing", "{}", "c2"),
                call("price_history", "not json", "c3"),
            ],
        },
        {"content": ANSWER},
    )
    sample = run_agent(model, agent_tools(View(ROWS, AT)), AT)
    assert sample.valid and json.loads(sample.decision) == {s: "neutral" for s in SYMBOLS}
    assert sample.calls == (
        ("symbol_snapshot", '{"symbol":"AAPL"}'),
        ("nothing", "{}"),
        ("price_history", "{}"),
    )
    assert sample.model_calls == 2
    fed = model.seen[1][0]
    assert fed[0]["content"].startswith(
        "You are a research analyst working at the instant 2025-02-03"
    )
    assert [m["tool_call_id"] for m in fed if m["role"] == "tool"] == ["c1", "c2", "c3"]
    assert fed[-2]["content"] == "Error: There is no tool called nothing."
    fewer = Model(
        {"content": None, "tool_calls": [call("symbol_snapshot", '{"symbol":"AAPL"}')]},
        {"content": ANSWER},
    )
    again = run_agent(fewer, agent_tools(View(ROWS, AT)), AT)
    assert again.decision == sample.decision and again.input_hash != sample.input_hash


def test_the_last_turn_has_no_tools_and_a_bad_answer_is_recorded():
    replies = [{"content": None, "tool_calls": [call("macro", "{}")]}] * (MAX_STEPS - 1)
    model = Model(*replies, {"content": "Mostly neutral."})
    sample = run_agent(model, agent_tools(View(ROWS, AT)), AT)
    assert model.seen[-1][1] is None and all(t == TOOLS for _, t in model.seen[:-1])
    assert not sample.valid and sample.error == "the answer has no JSON object"
    assert sample.answer == "Mostly neutral." and sample.model_calls == MAX_STEPS


def test_the_strategy_decides_the_same_every_time():
    rows = [r for r in ROWS if r[1] != "unrate"]
    times = decision_times(date(2025, 1, 5), date(2025, 2, 3))
    first, second = (strategy_sample(rows, times, GRID[0]) for _ in range(2))
    assert first == second and first.valid
    assert set(json.loads(first.decision)["weights"]) == set(SYMBOLS)


def sampled(*decisions):
    return [Sample(d, "i", (), 1, None if d else "bad") for d in decisions]


def test_the_mode_breaks_ties_the_same_way():
    assert mode(["b", "a", None, None]) == "a"
    assert mode(["b", "a", "b"]) == "b"
    assert mode([None]) is None


X = {"AAPL": "neutral", "BTCUSDT": "overweight", "MSFT": "overweight", "NVDA": "neutral",
     "SPY": "underweight"}  # fmt: skip
Y = {**X, "BTCUSDT": "underweight", "NVDA": "overweight"}
x, y = canonical(X), canonical(Y)


def test_symbols_that_flip_are_unsettled_and_the_rest_must_reproduce():
    found = summarize([sampled(x, x, y, x, x), sampled(y, y, y, x, x)], 0.8)
    expected = canonical({**X, "BTCUSDT": "unsettled", "NVDA": "unsettled"})
    assert found.decision == expected and found.rounds == (expected, expected)
    assert found.settled == {"AAPL": "neutral", "MSFT": "overweight", "SPY": "underweight"}
    assert found.parts["BTCUSDT"] == pytest.approx(0.6) and found.parts["AAPL"] == 1.0
    assert found.agreement == pytest.approx(0.6) and (found.distinct, found.invalid) == (2, 0)
    assert found.reproducible(0.8)
    assert summarize([sampled(x, x, y, x, x), sampled(y, y, y, x, x)], 0.5).decision == x


def test_a_settled_part_must_agree_in_every_round():
    z = canonical({**X, "AAPL": "overweight"})
    split = summarize([sampled(x, x, x, x, x, x), sampled(x, z, z)], 0.7)
    assert json.loads(split.decision)["AAPL"] == "neutral"
    assert split.rounds[1] != split.decision and not split.reproducible(0.7)


def test_too_many_invalid_answers_or_nothing_settled_fails():
    assert not summarize([sampled(x, x, x, None, None)], 0.8).reproducible(0.8)
    assert summarize([sampled(x, x, x, x, None)], 0.8).reproducible(0.8)
    nothing = summarize([sampled(x, y)], 0.8)
    assert nothing.settled == {k: v for k, v in X.items() if X[k] == Y[k]}
    assert not summarize([sampled(None)], 0.8).reproducible(0.0)
    flipped = canonical({k: STANCES[(STANCES.index(v) + 1) % 3] for k, v in X.items()})
    assert not summarize([sampled(x, flipped)], 0.8).reproducible(0.8)


def test_job_arguments():
    args = parse_args(["--catalog", "pitlake_prod", "--endpoint", "e", "--run-id", "7"])
    assert args.deciders == ("strategy", "agent")
    assert (args.samples, args.rounds, args.min_agreement, args.at) == (10, 2, 0.8, None)
    args = parse_args(
        ["--catalog", "pitlake_prod", "--endpoint", "e", "--deciders", "agent", "--at",
         "2026-06-01", "--withhold", "bar:AAPL/2026-06-15"]
    )  # fmt: skip
    assert args.deciders == ("agent",) and args.at == date(2026, 6, 1)
    assert args.withhold == ("bar:AAPL/2026-06-15",)
    for bad in (["--deciders", "oracle"], ["--samples", "0"], ["--min-agreement", "1.5"]):
        with pytest.raises(SystemExit):
            parse_args(["--catalog", "pitlake_prod", "--endpoint", "e", *bad])


RECORD = {
    "backtest_id": "b",
    "research_run_id": "r",
    "period_start": date(2025, 2, 1),
    "period_end": date(2026, 10, 8),
    "input_hash": "h",
    "params": GRID[0].to_json(),
}


def test_the_instant_is_a_decision_of_the_backtest():
    at, times = instant(RECORD, None)
    assert at == datetime(2026, 10, 8, 6, tzinfo=UTC) and times[0].date() == date(2025, 2, 1)
    assert instant(RECORD, date(2026, 6, 1))[0] == datetime(2026, 6, 1, 6, tzinfo=UTC)
    with pytest.raises(ValueError, match="outside"):
        instant(RECORD, date(2027, 1, 1))


def columns(schema: str) -> list[str]:
    return re.findall(r"(\w+) (?:MAP<[^>]*>|ARRAY<[^>]*>|\w+)", schema)


def decided(replay_of=None, expected=None) -> Decided:
    a = canonical({s: "neutral" for s in SYMBOLS})
    rounds = [sampled(a, a), sampled(a, None)]
    spec = Spec("agent", RECORD, None, ("filing:x-1",), 2, 2, replay_of, expected, "c")
    return Decided(spec, "p", agent_config("e"), "c", AT, {"gold.features": 4}, "s", 10,
                   rounds, summarize(rounds, 0.8), None)  # fmt: skip


def test_records_match_their_tables():
    args = parse_args(["--catalog", "pitlake_prod", "--endpoint", "e", "--run-id", "7"])
    found = decided()
    row = dict(zip(columns(DECISION_POINTS_SCHEMA), point_row(args, found, AT), strict=True))
    assert (row["decider"], row["withheld"], row["samples"], row["rounds"]) == (
        "agent",
        ["filing:x-1"],
        2,
        2,
    )
    assert row["agreement"] == 0.75 and row["reproducible"] is False and row["reproduced"] is None
    assert json.loads(row["config"])["temperature"] == 0.0
    samples = sample_rows(args, found, AT)
    assert len(samples) == 4
    named = dict(zip(columns(DECISION_SAMPLES_SCHEMA), samples[-1], strict=True))
    assert (named["round_index"], named["sample_index"], named["error"]) == (1, 1, "bad")
    assert named["decision_hash"] is None and json.loads(named["tool_calls"]) == []


def test_a_replay_must_give_the_recorded_decision_with_the_same_configuration():
    a = canonical({s: "neutral" for s in SYMBOLS})
    assert decided("p0", a).reproduced() is True
    assert decided("p0", "other").reproduced() is False
    assert "WHERE p.run_id = '42' AND p.replay_of IS NULL" in replay_query("pitlake_prod", "42")
    assert point_id("c", AT, {"b": 1, "a": 2}, ("y", "x")) == point_id(
        "c", AT, {"a": 2, "b": 1}, ("x", "y")
    )


def test_the_configurations_name_what_decides():
    assert strategy_config(GRID[0])["params"] == GRID[0].to_json()
    assert agent_config("e")["features"] == list(AGENT_REACH.features)
    assert in_reach(ROWS, AGENT_REACH) == ROWS


def test_the_job_runs_the_entry_point():
    with open("resources/jobs.yml") as found:
        job = yaml.safe_load(found)["resources"]["jobs"]["harness"]
    with open("pyproject.toml", "rb") as found:
        scripts = tomllib.load(found)["project"]["scripts"]
    (task,) = job["tasks"]
    assert task["task_key"] == "decide" and task["max_retries"] == 0
    assert scripts[task["python_wheel_task"]["entry_point"]] == "pitlake.jobs.decide:main"
    assert "${var.chat_endpoint}" in task["python_wheel_task"]["parameters"]
    names = {p["name"] for p in job["parameters"]}
    assert names == {"backtest", "at", "withhold", "deciders", "samples", "rounds",
                     "min_agreement", "replay"}  # fmt: skip

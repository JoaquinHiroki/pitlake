import json

import pytest

import agent
import digest

AS_OF = "2025-01-08T00:00:00Z"


def call(name, arguments, id_="c1"):
    return {
        "id": id_,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(arguments)},
    }


class Model:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.seen = []

    def __call__(self, messages, tools):
        self.seen.append(([dict(m) for m in messages], tools))
        return self.replies.pop(0)


def test_the_agent_calls_tools_feeds_back_their_results_and_answers():
    model = Model(
        {"content": None, "tool_calls": [call("symbol_digest", {"symbol": "AAPL"})]},
        {"content": "Apple closed at 245.00 on 2025-01-06."},
    )
    state = agent.Run("r1", AS_OF)
    tools = {"symbol_digest": lambda arguments: (f"facts of {arguments['symbol']}", None)}
    agent.run(state, [{"role": "user", "content": "How is Apple?"}], model, tools)
    assert state.status == "done" and state.answer.startswith("Apple closed")
    assert state.steps == [
        {
            "tool": "symbol_digest",
            "arguments": {"symbol": "AAPL"},
            "done": True,
            "result": "facts of AAPL",
            "view": None,
        }
    ]
    messages, tools_offered = model.seen[1]
    assert messages[0]["role"] == "system" and "2025-01-08 00:00 UTC" in messages[0]["content"]
    assert messages[-1] == {"role": "tool", "tool_call_id": "c1", "content": "facts of AAPL"}
    assert messages[-2]["tool_calls"][0]["id"] == "c1"
    assert [t["function"]["name"] for t in tools_offered] == [
        "symbol_digest",
        "price_history",
        "compare_symbols",
        "my_portfolio",
        "macro",
    ]
    assert state.warnings == [] and state.view()["done"] is True


def test_no_tool_takes_a_date_so_the_instant_stays_locked():
    for tool in agent.TOOLS:
        properties = tool["function"]["parameters"]["properties"]
        assert not {"as_of", "date", "instant", "start", "end"} & set(properties)


def test_tool_errors_go_back_to_the_model_but_leaks_stop_the_run():
    def broken(arguments):
        raise ValueError("Compare two or three symbols.")

    model = Model(
        {"tool_calls": [call("compare_symbols", {"symbols": ["AAPL"]}), call("weather", {}, "c2")]},
        {"content": "I need two symbols."},
    )
    state = agent.Run("r2", AS_OF)
    agent.run(state, [{"role": "user", "content": "Compare"}], model, {"compare_symbols": broken})
    assert [s.get("error") for s in state.steps] == [
        "Compare two or three symbols.",
        "There is no tool called weather.",
    ]
    assert model.seen[1][0][-1]["content"] == "Error: There is no tool called weather."

    def leaking(arguments):
        raise digest.LeakageFound("revenue_q became public at 2025-01-09")

    model = Model({"tool_calls": [call("symbol_digest", {"symbol": "AAPL"})]})
    with pytest.raises(digest.LeakageFound):
        agent.run(
            agent.Run("r3", AS_OF),
            [{"role": "user", "content": "x"}],
            model,
            {"symbol_digest": leaking},
        )


def test_the_last_step_offers_no_tools_so_the_model_must_answer():
    loop = {"tool_calls": [call("macro", {})]}
    model = Model(*([loop] * (agent.MAX_STEPS - 1)), {"content": "Done."})
    state = agent.Run("r4", AS_OF)
    agent.run(state, [{"role": "user", "content": "x"}], model, {"macro": lambda a: ("m", None)})
    assert state.answer == "Done." and model.seen[-1][1] is None
    assert len(state.steps) == agent.MAX_STEPS - 1


def test_dates_after_the_instant_are_flagged_unless_the_user_named_them():
    text = "Known on 2025-01-07; the 2025-01-10 close is after the instant; 2025-02-01 too."
    assert agent.later_dates(text, AS_OF, asked="What about 2025-01-10?") == [
        "The answer mentions 2025-02-01, after the instant."
    ]


def test_tool_arguments_are_bounded():
    assert agent.days({"days": 1000}) == 180 and agent.days({"days": "x"}) == 60
    assert agent.symbols({"symbols": ["AAPL", "SPY", "AAPL"]}) == ["AAPL", "SPY"]
    with pytest.raises(ValueError, match="two or three"):
        agent.symbols({"symbols": ["AAPL"]})
    with pytest.raises(ValueError, match="No digest"):
        agent.symbols({"symbols": ["AAPL", "TSLA"]})


def test_histories_keep_known_closes_and_index_from_a_common_day():
    bars = [
        {"session_date": "2025-01-02", "close": 100.0, "known": True},
        {"session_date": "2025-01-03", "close": 110.0, "known": True},
        {"session_date": "2025-01-06", "close": 120.0, "known": False},
    ]
    points = agent.history(bars, 30)
    assert points == [("2025-01-02", 100.0), ("2025-01-03", 110.0)]
    assert "from 100.00 to 110.00 (+10.00%)" in agent.history_text("AAPL", points)
    assert "No close" in agent.history_text("AAPL", [])
    found = agent.indexed({"A": points, "B": [("2025-01-03", 50.0), ("2025-01-04", 55.0)]})
    assert found["A"] == [("2025-01-03", 100.0)]
    assert found["B"] == [("2025-01-03", 100.0), ("2025-01-04", pytest.approx(110.0))]

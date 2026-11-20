import gzip
import json
import statistics
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import fixture
import scripted
from pitlake_counterfactual import cli
from pitlake_counterfactual.ablate import (
    BROKEN,
    CHANGED,
    FLIPPED,
    SAME,
    UNCHANGED,
    UNSETTLED,
    ablate,
    base_of,
    effect_of,
    fisher,
)
from pitlake_counterfactual.agent import (
    ANSWER_TOOL,
    MAX_STEPS,
    Sample,
    Session,
    agent_config,
    answered_by_tool,
    canonical,
    loop,
    parse_decision,
    prompts,
    run,
    tools_for,
)
from pitlake_counterfactual.endpoint import chat, load
from pitlake_counterfactual.world import (
    Bundle,
    as_of,
    day_micros,
    fingerprint,
    inputs_hash,
    micros,
)

MID_AAPL = "bar:AAPL/2025-01-02"
UNRATE = "release:UNRATE/2025-01-10"
FILING = f"filing:{fixture.FILING}"


@pytest.fixture(scope="module")
def bundle():
    return Bundle(fixture.payload())


def complete():
    return loop(scripted.complete)


def rows_of(rows, feature, entity):
    return [r for r in rows if r[1] == feature and r[0] == entity]


def test_the_bundle_verifies_and_counts_its_items(bundle):
    assert bundle.check() == []
    assert bundle.by_kind() == {"bar": 187, "filing": 1, "release": 3}
    assert bundle.set_hash == bundle.recorded_set_hash
    assert bundle.id == bundle.recorded_id
    assert bundle.at == fixture.AT


def test_a_bundle_reads_from_json_and_gzip(tmp_path):
    plain = fixture.write(tmp_path / "world.json")
    packed = tmp_path / "world.json.gz"
    packed.write_bytes(gzip.compress(plain.read_bytes()))
    assert Bundle.read(plain).id == Bundle.read(packed).id


def test_a_changed_bar_breaks_the_set_and_the_rebuild():
    payload = fixture.payload()
    payload["bars"][5][6] = "1.0000000000"
    problems = Bundle(payload).check()
    assert any("content hashes" in p for p in problems)
    assert any("the items hash" in p for p in problems)
    assert any("close of BTCUSDT" in p for p in problems)


def test_another_format_is_refused():
    payload = fixture.payload()
    payload["format"] = "pitlake-world/2"
    with pytest.raises(ValueError, match="not a pitlake-world/1 bundle"):
        Bundle(payload)


def test_a_row_without_an_item_is_reported():
    payload = fixture.payload()
    payload["rows"].append(["AAPL", "revenue_q", 0, 0, 1.0, "edgar_company_facts", "0000-x"])
    payload.pop("id")
    assert any("not an item" in p for p in Bundle(payload).check())


def test_withholding_the_last_bar_removes_its_rows_only(bundle):
    world = bundle.withhold([fixture.LAST_AAPL])
    assert world.coherent
    assert (world.removed, world.recomputed) == (2, 0)
    assert not [r for r in world.rows if r[0] == "AAPL" and r[6] == "2025-01-31"]
    assert len(world.rows) == len(bundle.rows) - 2


def test_withholding_a_bar_recomputes_the_sessions_after_it(bundle):
    world = bundle.withhold([MID_AAPL])
    assert world.coherent
    days = bundle.days["AAPL"]
    after = days[days.index("2025-01-02") + 1 : days.index("2025-01-02") + 21]
    assert world.recomputed > 0 and world.removed == world.recomputed + 2
    assert len(world.rows) == len(bundle.rows) - 2
    before = {(r[1], r[6]): r[4] for r in rows_of(bundle.rows, "volatility_20d", "AAPL")}
    now = {(r[1], r[6]): r[4] for r in rows_of(world.rows, "volatility_20d", "AAPL")}
    assert set(before) - set(now) <= {("volatility_20d", d) for d in [*after, "2025-01-02"]}
    assert any(now[k] != before[k] for k in now if k[1] in after)
    later = [k for k in now if k[1] > after[-1]]
    assert later and all(now[k] == before[k] for k in later)


def test_volatility_is_the_sample_deviation_of_twenty_log_returns(bundle):
    import math

    closes = [float(b.prices[3]) for b in bundle.bars if b.symbol == "MSFT"]
    returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    last = rows_of(bundle.rows, "volatility_20d", "MSFT")[-1]
    assert last[4] == pytest.approx(statistics.stdev(returns[-20:]), rel=1e-12)
    assert len(rows_of(bundle.rows, "volatility_20d", "MSFT")) == len(closes) - 20


def test_a_price_row_is_knowable_when_the_last_bar_of_its_window_is(bundle):
    bars = {b.day: b.knowable_at for b in bundle.bars if b.symbol == "NVDA"}
    for row in rows_of(bundle.rows, "close", "NVDA"):
        assert row[3] == bars[row[6]]


def test_withholding_a_filing_or_release_removes_its_rows(bundle):
    world = bundle.withhold([FILING, UNRATE])
    assert world.coherent and world.recomputed == 0
    assert not [
        r for r in world.rows if r[5] != "bars_1d" and r[6] in (fixture.FILING, "2025-01-10")
    ]
    assert world.removed == 5


def test_withholding_an_unknown_item_is_refused(bundle):
    with pytest.raises(ValueError, match="cannot withhold"):
        bundle.withhold(["bar:AAPL/2030-01-01"])


def test_the_null_world_is_the_bundle(bundle):
    world = bundle.withhold(())
    assert world.rows == bundle.rows and world.coherent and world.hash == bundle.withhold([]).hash


def test_as_of_gives_macro_to_every_symbol_and_hides_a_null_value(bundle):
    seen = as_of(bundle.rows, bundle.symbols, "US", bundle.at)
    unrate = [s for s in seen if s[1] == "unrate"]
    assert {s[0] for s in unrate} == set(bundle.symbols) and unrate[0][2] == 4.1
    hidden = [
        *bundle.rows,
        (
            "US",
            "unrate",
            day_micros("2024-12-01"),
            micros(bundle.at),
            None,
            "fred_series_vintages",
            "2025-02-01",
        ),
    ]
    assert not [s for s in as_of(hidden, bundle.symbols, "US", bundle.at) if s[1] == "unrate"]
    assert inputs_hash(bundle.rows, bundle.symbols, "US", bundle.at) != inputs_hash(
        hidden, bundle.symbols, "US", bundle.at
    )


def test_a_world_exports_what_the_independent_check_reads(bundle, tmp_path):
    world = bundle.withhold([MID_AAPL])
    folder = world.export(tmp_path / "w", "world-1")
    meta = dict(line.split("\t") for line in (folder / "meta.tsv").read_text().splitlines())
    assert meta["input_hash"] == world.inputs_hash()
    assert meta["period_start"] == meta["period_end"] == "2025-02-01"
    assert meta["decision_hour"] == "6" and meta["universe"] == ",".join(bundle.symbols)
    lines = (folder / "features.tsv").read_text().splitlines()
    assert len(lines) == len(world.rows) and all(len(x.split("\t")) == 7 for x in lines)
    bars = (folder / "bars.tsv").read_text().splitlines()
    assert len(bars) == len(bundle.bars) - 1 and "AAPL\t2025-01-02" not in "\n".join(bars)
    assert (folder / "withheld.tsv").read_text() == f"{MID_AAPL}\n"


def test_the_tools_read_only_the_world(bundle):
    session = Session.of(bundle.withhold([fixture.LAST_AAPL]))
    text = session.call("symbol_snapshot", {"symbol": "AAPL"})
    assert "last close 239.00 on 2025-01-30" in text
    assert session.calls == [("symbol_snapshot", '{"symbol":"AAPL"}')]
    assert session.call("symbol_snapshot", {"symbol": "TSLA"}).startswith("Error: No symbol")
    assert session.call("news", {}) == "Error: There is no tool called news."
    assert "Fed funds rate for 2024-12: 4.48%" in session.call("macro")
    assert "2025-01-30 239.00" in session.call("price_history", {"symbol": "AAPL", "days": 2})


def test_the_task_names_the_symbols_and_the_instant(bundle):
    system, user = prompts(bundle.at, bundle.symbols)
    assert "2025-02-01 06:00 UTC" in system["content"]
    assert "BTCUSDT, SPY, AAPL, MSFT, NVDA" in user["content"]
    names = [t["function"]["name"] for t in tools_for(bundle.symbols)]
    assert names == ["symbol_snapshot", "price_history", "macro"]
    config = agent_config("m", bundle.symbols, answer_by_tool=True)
    assert config["answer"]["function"]["name"] == ANSWER_TOOL
    assert "answer" not in agent_config("m", bundle.symbols)


def test_a_decision_rates_every_symbol(bundle):
    symbols = bundle.symbols
    rated = {s: "Neutral " for s in symbols}
    assert set(parse_decision(json.dumps(rated), symbols).values()) == {"neutral"}
    with pytest.raises(ValueError, match="no JSON"):
        parse_decision("I think AAPL", symbols)
    with pytest.raises(ValueError, match="outside the task"):
        parse_decision({**rated, "TSLA": "neutral"}, symbols)
    with pytest.raises(ValueError, match="AAPL is rated"):
        parse_decision({**rated, "AAPL": "buy"}, symbols)


def test_the_loop_runs_the_tools_and_records_the_inputs(bundle):
    sample = run(complete(), bundle.withhold(()))
    assert sample.valid and json.loads(sample.decision)["AAPL"] == "overweight"
    assert sample.model_calls == 2 and sample.calls == (("symbol_snapshot", '{"symbol":"AAPL"}'),)
    session = Session.of(bundle.withhold(()))
    text = session.call("symbol_snapshot", {"symbol": "AAPL"})
    assert sample.input_hash == fingerprint([["symbol_snapshot", '{"symbol":"AAPL"}', text]])


def test_an_agent_that_never_answers_runs_out_of_steps(bundle):
    def asks(messages, tools):
        call = {"id": "c", "function": {"name": "macro", "arguments": "{}"}}
        return {"role": "assistant", "content": "", "tool_calls": [call]}

    sample = run(loop(asks), bundle.withhold(()))
    assert not sample.valid and sample.error == "no answer within the steps"
    assert sample.model_calls == MAX_STEPS


def test_an_agent_that_fails_gives_an_invalid_sample(bundle):
    def fails(session):
        raise RuntimeError("down")

    sample = run(fails, bundle.withhold(()))
    assert not sample.valid and sample.error == "RuntimeError: down"


def test_a_model_can_answer_through_the_decide_tool(bundle):
    seen = []

    def call(messages, tools, choice):
        seen.append((None if tools is None else [t["function"]["name"] for t in tools], choice))
        if not any(m["role"] == "tool" for m in messages):
            request = {"id": "a", "function": {"name": "macro", "arguments": "{}"}}
            return {"role": "assistant", "tool_calls": [request]}
        answer = json.dumps({s: "neutral" for s in bundle.symbols})
        request = {"id": "b", "function": {"name": ANSWER_TOOL, "arguments": answer}}
        return {"role": "assistant", "tool_calls": [request]}

    sample = run(loop(answered_by_tool(call, bundle.symbols)), bundle.withhold(()))
    assert sample.valid and sample.model_calls == 2
    assert seen[0] == (["symbol_snapshot", "price_history", "macro", ANSWER_TOOL], "required")


def test_fisher_is_one_for_equal_samples_and_small_for_opposite_ones():
    assert fisher(10, 10, 10, 10) == 1.0
    assert fisher(20, 20, 0, 10) < 1e-6
    assert fisher(0, 0, 3, 5) == 1.0


def sample(decision):
    return Sample(None if decision is None else canonical(decision), "", (), 1)


def test_a_part_settles_when_enough_samples_agree():
    a = {"AAPL": "overweight", "MSFT": "neutral"}
    b = {"AAPL": "overweight", "MSFT": "underweight"}
    base = base_of([[sample(a)] * 5, [sample(a)] * 4 + [sample(b)]], 0.8)
    assert base.reproducible and base.settled == {"AAPL": "overweight", "MSFT": "neutral"}
    assert base.support == {"AAPL": 10, "MSFT": 9}
    split = base_of([[sample(a)] * 5, [sample(b)] * 5], 0.8)
    assert split.settled == {"AAPL": "overweight"} and split.parts["MSFT"] == 0.5
    unstable = base_of([[sample(a)] * 9 + [sample(b)], [sample(b)] * 3 + [sample(a)] * 2], 0.6)
    assert unstable.settled["MSFT"] == "neutral" and not unstable.stable
    assert not base_of([[sample(None)] * 3], 0.8).reproducible


def test_an_effect_compares_each_settled_part():
    a = {"AAPL": "overweight", "MSFT": "neutral"}
    base = base_of([[sample(a)] * 10, [sample(a)] * 10], 0.8)
    flipped = effect_of(base, [sample({**a, "AAPL": "underweight"})] * 10, 0.8)
    assert flipped.outcomes == {"AAPL": FLIPPED, "MSFT": SAME} and flipped.caused == ("AAPL",)
    assert flipped.effects["AAPL"] == 1.0
    mixed = [sample({**a, "AAPL": "underweight"})] * 3 + [sample(a)] * 7
    assert effect_of(base, mixed, 0.8).outcomes["AAPL"] == UNSETTLED
    invalid = effect_of(base, [sample(None)] * 10, 0.8)
    assert not invalid.valid and invalid.outcomes["AAPL"] == "invalid"


def test_an_ablation_finds_the_bar_the_agent_read(bundle):
    worlds = [(fixture.LAST_AAPL,), ("bar:MSFT/2025-01-31",)]
    report = ablate(bundle, worlds, complete(), {}, samples=4, workers=2)
    assert report.passed and report.null_passed
    assert report.base.settled["AAPL"] == "overweight" and report.base.samples == 8
    by_name = {t.name: t for t in report.worlds}
    aapl = by_name[fixture.LAST_AAPL]
    assert aapl.verdict == CHANGED and aapl.inputs_changed and aapl.effect.caused == ("AAPL",)
    msft = by_name["bar:MSFT/2025-01-31"]
    assert msft.verdict == UNCHANGED and not msft.inputs_changed
    found = report.to_json("test")
    assert found["passed"] and [w["verdict"] for w in found["worlds"]] == [CHANGED, UNCHANGED]
    assert found["bundle"]["id"] == bundle.id and len(found["base"]["rounds"]) == 2


def test_a_random_agent_fails_the_null_world(bundle):
    import itertools

    flips = itertools.cycle(["overweight"] * 8 + ["underweight"] * 4)
    lock = threading.Lock()

    def decide(session):
        with lock:
            stance = next(flips)
        return {s: stance if s == "AAPL" else "neutral" for s in session.symbols}

    report = ablate(bundle, [], decide, {}, samples=4, workers=1)
    assert report.null is not None and report.null.verdict != UNCHANGED
    assert not report.passed


def test_an_incoherent_world_is_invalid(bundle):
    from dataclasses import replace

    from pitlake_counterfactual.ablate import Tested

    world = replace(bundle.withhold([MID_AAPL]), problems=("broken",))
    effect = effect_of(
        base_of([[sample({"AAPL": "neutral"})]], 0.8), [sample({"AAPL": "neutral"})], 0.8
    )
    assert Tested("x", world, [], effect, False).verdict == BROKEN


def test_the_command_ablates_with_a_chat_function(bundle, tmp_path, monkeypatch, capsys):
    path = fixture.write(tmp_path / "world.json")
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "result"
    argv = [
        "ablate",
        str(path),
        "--withhold",
        fixture.LAST_AAPL,
        "--agent",
        "scripted:complete",
        "--samples",
        "3",
        "--out",
        str(out),
    ]
    with pytest.raises(SystemExit) as stopped:
        cli.main(argv)
    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "VERIFIED" in printed and "SAME: null world" in printed
    assert f"CHANGED: without {fixture.LAST_AAPL}" in printed and "OK: 1 worlds" in printed
    result = json.loads((out / "result.json").read_text())
    assert result["passed"] and result["config"]["endpoint"] == "scripted:complete"
    assert (out / "worlds" / "real" / "features.tsv").exists()
    assert (out / "worlds" / "world-1" / "withheld.tsv").read_text() == f"{fixture.LAST_AAPL}\n"


def test_the_command_ablates_with_a_deciding_function(tmp_path, capsys):
    path = fixture.write(tmp_path / "world.json")
    agent = tmp_path / "mine.py"
    agent.write_text(open(scripted.__file__).read())
    argv = [
        "ablate",
        str(path),
        "--withhold",
        f"{FILING} {UNRATE}",
        "--decider",
        f"{agent}:decide",
        "--samples",
        "2",
        "--no-null",
        "--out",
        str(tmp_path / "r"),
    ]
    with pytest.raises(SystemExit) as stopped:
        cli.main(argv)
    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "null world" not in printed and f"SAME: without {FILING} {UNRATE}" in printed


def test_the_command_refuses_an_unknown_item(tmp_path):
    path = fixture.write(tmp_path / "world.json")
    argv = [
        "ablate",
        str(path),
        "--withhold",
        "bar:AAPL/2030-01-01",
        "--decider",
        "x:y",
        "--out",
        str(tmp_path / "r"),
    ]
    with pytest.raises(SystemExit, match="not items of"):
        cli.main(argv)


def test_the_command_inspects_and_builds_a_world(tmp_path, capsys):
    path = fixture.write(tmp_path / "world.json")
    with pytest.raises(SystemExit) as stopped:
        cli.main(["inspect", str(path), "--kind", "release"])
    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert "release:UNRATE/2025-01-10\tUS" in printed and "bar:" not in printed.split("VERIFIED")[1]
    with pytest.raises(SystemExit) as stopped:
        cli.main(
            ["world", str(path), "--withhold", MID_AAPL, "--show", "--out", str(tmp_path / "w")]
        )
    assert stopped.value.code == 0
    printed = capsys.readouterr().out
    assert f"withheld {MID_AAPL}" in printed and "coherent" in printed
    assert "Fed funds rate" in printed and (tmp_path / "w" / "meta.tsv").exists()


class Fake(BaseHTTPRequestHandler):
    bodies: list = []
    failures = 0

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        Fake.bodies.append((self.path, self.headers.get("Authorization"), body))
        if Fake.failures:
            Fake.failures -= 1
            self.send_response(503)
            self.end_headers()
            return
        reply = {"choices": [{"message": {"role": "assistant", "content": "{}"}}]}
        data = json.dumps(reply).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def test_a_chat_endpoint_gets_the_task_settings_and_is_retried(monkeypatch):
    monkeypatch.setattr("pitlake_counterfactual.endpoint.time.sleep", lambda s: None)
    server = HTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        Fake.bodies, Fake.failures = [], 1
        call = chat(f"http://127.0.0.1:{server.server_port}/v1/", "m1", "k")
        reply = call([{"role": "user", "content": "hi"}], [{"type": "function"}], "required")
    finally:
        server.shutdown()
    assert reply == {"role": "assistant", "content": "{}"}
    assert len(Fake.bodies) == 2
    path, key, body = Fake.bodies[-1]
    assert path == "/v1/chat/completions" and key == "Bearer k"
    assert body["model"] == "m1" and body["temperature"] == 0.0 and body["max_tokens"] == 700
    assert body["tool_choice"] == "required" and body["tools"] == [{"type": "function"}]


def test_a_function_loads_from_a_module_or_a_file(tmp_path):
    agent = tmp_path / "agent_file.py"
    agent.write_text("def go(x):\n    return x\n")
    assert load(f"{agent}:go")(3) == 3
    assert load("scripted:decide") is scripted.decide
    with pytest.raises(ValueError, match="module:function"):
        load("scripted")
    with pytest.raises(ValueError, match="not a function"):
        load("scripted:SYMBOLS")

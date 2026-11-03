from types import SimpleNamespace

import pytest
from databricks.sdk.errors import TooManyRequests
from fastapi.testclient import TestClient

import server

REGISTRY = {
    "columns": [{"name": "dataset", "type": "STRING"}],
    "rows": [["binance.spot_trades"], ["coinbase.candles_1m"]],
    "row_count": 2,
    "truncated": False,
}


class Genie:
    def __init__(self):
        self.calls = []
        self.limited = False

    def start_conversation(self, space_id, content):
        if self.limited:
            raise TooManyRequests("RESOURCE_EXHAUSTED")
        self.calls.append(("start", space_id, content))
        return SimpleNamespace(conversation_id="c1", message_id="m1")

    def create_message(self, space_id, conversation_id, content):
        self.calls.append(("follow_up", space_id, conversation_id, content))
        return SimpleNamespace(conversation_id=conversation_id, message_id="m2")

    def get_message(self, space_id, conversation_id, message_id):
        found = {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "status": "ASKING_AI",
        }
        return SimpleNamespace(as_dict=lambda: found)


class Jobs:
    def __init__(self):
        self.started = []

    def run_now(self, job_id, job_parameters):
        self.started.append((job_id, job_parameters))
        return SimpleNamespace(run_id=7)


@pytest.fixture
def app(monkeypatch):
    fake = SimpleNamespace(
        genie=Genie(),
        jobs=Jobs(),
        config=SimpleNamespace(host="https://example.cloud.databricks.com/"),
        queries=[],
    )

    def run_query(statement, parameters):
        fake.queries.append((statement, [(p.name, p.value, p.type) for p in parameters]))
        return REGISTRY

    found = server.Settings("pitlake_dev", "warehouse", 42, {"markets": "sm", "operations": "so"})
    monkeypatch.setattr(server, "workspace", lambda: fake)
    monkeypatch.setattr(server, "settings", lambda: found)
    monkeypatch.setattr(server, "run_query", run_query)
    server._results.clear()
    fake.client = TestClient(server.app)
    return fake


def test_config_lists_both_spaces_with_their_starters(app):
    found = app.client.get("/api/config").json()
    assert found["catalog"] == "pitlake_dev" and found["host"].endswith(".com")
    assert [s["key"] for s in found["spaces"]] == ["markets", "operations"]
    assert all(s["starters"] for s in found["spaces"])


def test_a_question_starts_a_conversation_and_a_follow_up_continues_it(app):
    first = app.client.post("/api/genie/markets/messages", json={"question": " Revenue? "})
    assert first.json() == {"conversation_id": "c1", "message_id": "m1"}
    follow_up = app.client.post(
        "/api/genie/markets/messages", json={"question": "And MSFT?", "conversation_id": "c1"}
    )
    assert follow_up.json() == {"conversation_id": "c1", "message_id": "m2"}
    assert app.genie.calls == [
        ("start", "sm", "Revenue?"),
        ("follow_up", "sm", "c1", "And MSFT?"),
    ]
    polled = app.client.get("/api/genie/markets/conversations/c1/messages/m2").json()
    assert polled["status"] == "ASKING_AI" and not polled["done"]


def test_questions_are_refused_for_unknown_spaces_blank_text_and_rate_limits(app):
    assert app.client.post("/api/genie/sales/messages", json={"question": "x"}).status_code == 404
    assert app.client.post("/api/genie/markets/messages", json={"question": " "}).status_code == 400
    app.genie.limited = True
    limited = app.client.post("/api/genie/markets/messages", json={"question": "x"})
    assert limited.status_code == 429 and "RESOURCE_EXHAUSTED" in limited.json()["detail"]


def test_views_need_their_parameters_and_are_cached_until_refreshed(app):
    assert app.client.get("/api/views/silver").status_code == 404
    assert app.client.get("/api/views/daily_rows?days=30").status_code == 400
    assert app.client.get("/api/views/rejections?days=thirty").status_code == 400
    path = "/api/views/daily_rows?silver_table=binance_spot_trades&days=30"
    assert app.client.get(path).json() == REGISTRY
    app.client.get(path)
    assert len(app.queries) == 1
    assert app.queries[0][1] == [
        ("silver_table", "binance_spot_trades", "STRING"),
        ("days", "30", "INT"),
    ]
    app.client.get(path + "&refresh=true")
    assert len(app.queries) == 2


def test_ingest_starts_the_job_only_for_registered_datasets(app):
    refused = app.client.post("/api/ingest", json={"datasets": ["edgar.filings"]})
    assert refused.status_code == 400 and "Not registered" in refused.json()["detail"]
    started = app.client.post(
        "/api/ingest",
        json={"datasets": ["coinbase.candles_1m"], "start": "2025-01-02", "end": "2025-01-03"},
    ).json()
    assert started == {"run_id": 7, "url": "https://example.cloud.databricks.com/jobs/42/runs/7"}
    assert app.jobs.started == [
        (
            42,
            {
                "datasets": "coinbase.candles_1m",
                "period_start": "2025-01-02",
                "period_end": "2025-01-03",
                "reload": "false",
                "max_files": "0",
            },
        )
    ]


def table(columns, rows):
    return {
        "columns": [{"name": c, "type": None} for c in columns],
        "rows": rows,
        "row_count": len(rows),
        "truncated": False,
    }


GOLD = {
    "SELECT venue, symbol": table(
        ["venue", "symbol", "first_session", "last_session"],
        [["alpaca", "AAPL", "2025-01-02", "2026-11-02"]],
    ),
    "SELECT session_date": table(
        ["session_date", "close", "volume", "knowable_at", "known"],
        [
            ["2025-07-31", 200.0, 1, "2025-07-31T20:00:00.000Z", True],
            ["2025-08-01", 202.0, 1, "2025-08-01T20:00:00.000Z", True],
            ["2025-08-04", 210.0, 1, "2025-08-04T20:00:00.000Z", False],
        ],
    ),
    "SELECT entity": table(
        ["entity", "feature_group", "feature", "event_time", "value", "knowable_at", "source_ref"],
        [["US", "macro", "unrate", "2025-06-01", 4.1, "2025-07-03T12:30:00.000Z", "2025-07-03"]],
    ),
    "WITH filings": table(
        ["form", "accession_number", "period_end", "knowable_at", "known"],
        [["10-Q", "a1", "2025-06-28", "2025-08-01T10:30:00.000Z", True]],
    ),
}


class Serving:
    def __init__(self):
        self.asked = []

    def query(self, name, messages, max_tokens, temperature):
        self.asked.append((name, [m.content for m in messages]))
        reply = SimpleNamespace(message=SimpleNamespace(content="Apple rose 1%."))
        return SimpleNamespace(choices=[reply])


@pytest.fixture
def gold(app, monkeypatch):
    def run_query(statement, parameters):
        app.queries.append((statement, [(p.name, p.value, p.type) for p in parameters]))
        return next(found for start, found in GOLD.items() if statement.startswith(start))

    monkeypatch.setattr(server, "run_query", run_query)
    app.serving_endpoints = Serving()
    server._summaries.clear()
    return app


def test_symbols_carry_the_sessions_gold_holds(gold):
    found = {s["symbol"]: s for s in gold.client.get("/api/digest/symbols").json()}
    assert found["AAPL"]["last_session"] == "2026-11-02"
    assert found["BTCUSDT"]["venue"] == "binance" and found["BTCUSDT"]["first_session"] is None


def test_a_digest_reads_gold_as_of_the_instant(gold):
    found = gold.client.get("/api/digest?symbol=AAPL&as_of=2025-08-02").json()
    assert found["as_of"] == "2025-08-02T00:00:00Z" and found["chat"] is False
    assert [b["known"] for b in found["bars"]] == [True, True, False]
    assert found["moves"]["last_close"] == 202.0
    assert found["snapshot"][0]["shown"] == "4.10%"
    assert ("as_of", "2025-08-02 00:00:00", "TIMESTAMP") in gold.queries[0][1]
    assert ("venue", "alpaca", "STRING") in gold.queries[0][1]
    assert gold.client.get("/api/digest?symbol=TSLA&as_of=2025-08-02").status_code == 400
    assert gold.client.get("/api/digest?symbol=AAPL&as_of=soon").status_code == 400


def test_a_summary_needs_a_chat_endpoint_and_is_written_once(gold, monkeypatch):
    body = {"symbol": "AAPL", "as_of": "2025-08-02"}
    assert gold.client.post("/api/digest/summary", json=body).status_code == 503
    found = server.Settings(
        "pitlake_dev", "warehouse", 42, {"markets": "sm", "operations": "so"}, "chat"
    )
    monkeypatch.setattr(server, "settings", lambda: found)
    summary = gold.client.post("/api/digest/summary", json=body).json()
    assert summary["text"] == "Apple rose 1%." and summary["endpoint"] == "chat"
    assert "Filed a 10-Q for the period ending 2025-06-28 on 2025-08-01." in summary["facts"]
    gold.client.post("/api/digest/summary", json=body)
    assert len(gold.serving_endpoints.asked) == 1
    assert "210" not in gold.serving_endpoints.asked[0][1][1]


def test_a_portfolio_is_valued_from_known_closes_and_its_digest_written_once(gold, monkeypatch):
    body = {
        "holdings": [{"symbol": "AAPL", "quantity": 10}, {"symbol": "SPY", "quantity": 2}],
        "as_of": "2025-08-02",
    }
    found = gold.client.post("/api/portfolio", json=body).json()
    assert found["value"] == 12 * 202.0 and found["last_day"] == "2025-08-01"
    assert [p["weight"] for p in found["positions"]] == pytest.approx([10 / 12, 2 / 12])
    assert [s["known"] for s in found["series"]] == [True, True, False]
    refused = gold.client.post("/api/portfolio", json=body | {"holdings": []})
    assert refused.status_code == 400
    assert gold.client.post("/api/portfolio/summary", json=body).status_code == 503
    chat = server.Settings(
        "pitlake_dev", "warehouse", 42, {"markets": "sm", "operations": "so"}, "chat"
    )
    monkeypatch.setattr(server, "settings", lambda: chat)
    server._portfolio_summaries.clear()
    summary = gold.client.post("/api/portfolio/summary", json=body).json()
    assert summary["text"] == "Apple rose 1%."
    assert "Portfolio value 2,424.00 USD from the closes known on 2025-08-01." in summary["facts"]
    gold.client.post("/api/portfolio/summary", json=body)
    assert len(gold.serving_endpoints.asked) == 1


class Invocations:
    def __init__(self):
        self.bodies = []

    def do(self, method, path, body):
        self.bodies.append((path, body))
        if len(self.bodies) == 1:
            call = {
                "id": "c1",
                "type": "function",
                "function": {
                    "name": "price_history",
                    "arguments": '{"symbol": "AAPL", "days": 30}',
                },
            }
            return {
                "choices": [
                    {"message": {"role": "assistant", "content": None, "tool_calls": [call]}}
                ]
            }
        return {"choices": [{"message": {"role": "assistant", "content": "AAPL rose to 202.00."}}]}


def test_the_agent_runs_in_the_background_on_the_locked_instant(gold, monkeypatch):
    body = {"as_of": "2025-08-02", "messages": [{"role": "user", "content": "How is Apple?"}]}
    assert gold.client.post("/api/agent", json=body).status_code == 503
    chat = server.Settings(
        "pitlake_dev", "warehouse", 42, {"markets": "sm", "operations": "so"}, "chat"
    )
    monkeypatch.setattr(server, "settings", lambda: chat)
    monkeypatch.setattr(server, "AGENT_POOL", SimpleNamespace(submit=lambda f, *args: f(*args)))
    gold.api_client = Invocations()
    assert gold.client.post("/api/agent", json=body | {"messages": []}).status_code == 400
    started = gold.client.post("/api/agent", json=body).json()
    assert started["as_of"] == "2025-08-02T00:00:00Z"
    run = gold.client.get(f"/api/agent/runs/{started['run_id']}").json()
    assert run["done"] and run["answer"] == "AAPL rose to 202.00." and run["error"] is None
    step = run["steps"][0]
    assert step["tool"] == "price_history" and "from 200.00 to 202.00" in step["result"]
    assert step["view"]["series"][0]["points"] == [["2025-07-31", 200.0], ["2025-08-01", 202.0]]
    path, sent = gold.api_client.bodies[0]
    assert path == "/serving-endpoints/chat/invocations" and len(sent["tools"]) == 5
    assert ("as_of", "2025-08-02 00:00:00", "TIMESTAMP") in gold.queries[0][1]
    assert gold.client.get("/api/agent/runs/missing").status_code == 404

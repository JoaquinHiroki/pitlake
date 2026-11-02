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

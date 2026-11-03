import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from typing import Literal

import uvicorn
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    DatabricksError,
    RequestLimitExceeded,
    ResourceExhausted,
    TooManyRequests,
)
from databricks.sdk.service.dashboards import GenieFeedbackRating
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementState,
)
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import console
import conversation
import digest

CACHE_SECONDS = 300
DIST = Path(__file__).resolve().parent / "dist"
RUNNING = {StatementState.PENDING, StatementState.RUNNING}


@dataclass(frozen=True)
class Settings:
    catalog: str
    warehouse_id: str
    ingest_job_id: int
    spaces: dict[str, str]
    chat_endpoint: str | None = None


@cache
def settings() -> Settings:
    return Settings(
        console.validate_catalog(os.environ["PITLAKE_CATALOG"]),
        os.environ["DATABRICKS_WAREHOUSE_ID"],
        int(os.environ["INGEST_JOB_ID"]),
        {key: os.environ[space.env] for key, space in conversation.SPACES.items()},
        os.environ.get("CHAT_ENDPOINT") or None,
    )


@cache
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


class Question(BaseModel):
    question: str
    conversation_id: str | None = None


class Feedback(BaseModel):
    rating: Literal["positive", "negative", "none"]


class DigestRequest(BaseModel):
    symbol: str
    as_of: str


class IngestRequest(BaseModel):
    datasets: list[str]
    start: date | None = None
    end: date | None = None
    reload: bool = False
    max_files: int = 0


app = FastAPI(title="PITLake console", docs_url="/api/docs", openapi_url="/api/openapi.json")


@app.exception_handler(DatabricksError)
def databricks_error(request: Request, error: DatabricksError) -> JSONResponse:
    limited = isinstance(error, TooManyRequests | RequestLimitExceeded | ResourceExhausted)
    return JSONResponse({"detail": str(error)}, status_code=429 if limited else 502)


@app.exception_handler(digest.LeakageFound)
def leakage_found(request: Request, error: digest.LeakageFound) -> JSONResponse:
    return JSONResponse({"detail": f"Refused a digest that would leak: {error}"}, status_code=500)


def run_query(statement: str, parameters: list[StatementParameterListItem]) -> dict:
    sql = workspace().statement_execution
    response = sql.execute_statement(
        statement,
        settings().warehouse_id,
        parameters=parameters,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    while response.status.state in RUNNING:
        time.sleep(1)
        response = sql.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        error = response.status.error
        raise HTTPException(502, error.message if error else response.status.state.value)
    return console.result_table(response.as_dict())


_results: dict[tuple, tuple[float, dict]] = {}


def cached_query(
    statement: str, parameters: list[StatementParameterListItem], refresh: bool = False
) -> dict:
    key = (statement, tuple((p.name, p.value) for p in parameters))
    hit = _results.get(key)
    if hit and not refresh and time.monotonic() - hit[0] < CACHE_SECONDS:
        return hit[1]
    result = run_query(statement, parameters)
    _results[key] = (time.monotonic(), result)
    return result


def space_id(space: str) -> str:
    if space not in settings().spaces:
        raise HTTPException(404, f"No Genie space called {space!r}.")
    return settings().spaces[space]


@app.get("/api/config")
def config() -> dict:
    found = settings()
    return {
        "catalog": found.catalog,
        "host": workspace().config.host.rstrip("/"),
        "ingest_job_id": found.ingest_job_id,
        "spaces": [
            {"key": key, "title": s.title, "caption": s.caption, "starters": list(s.starters)}
            for key, s in conversation.SPACES.items()
        ],
    }


@app.post("/api/genie/{space}/messages")
def ask(space: str, body: Question) -> dict:
    question = body.question.strip()
    if not question:
        raise HTTPException(400, "Write a question first.")
    genie = workspace().genie
    if body.conversation_id:
        waiter = genie.create_message(space_id(space), body.conversation_id, question)
    else:
        waiter = genie.start_conversation(space_id(space), question)
    return {"conversation_id": waiter.conversation_id, "message_id": waiter.message_id}


@app.get("/api/genie/{space}/conversations/{conversation_id}/messages/{message_id}")
def message(space: str, conversation_id: str, message_id: str) -> dict:
    found = workspace().genie.get_message(space_id(space), conversation_id, message_id)
    return conversation.message_view(found.as_dict())


@app.get(
    "/api/genie/{space}/conversations/{conversation_id}/messages/{message_id}"
    "/attachments/{attachment_id}/result"
)
def attachment_result(space: str, conversation_id: str, message_id: str, attachment_id: str):
    found = workspace().genie.get_message_attachment_query_result(
        space_id(space), conversation_id, message_id, attachment_id
    )
    statement = found.statement_response
    return console.result_table(statement.as_dict() if statement else {})


@app.post("/api/genie/{space}/conversations/{conversation_id}/messages/{message_id}/feedback")
def feedback(space: str, conversation_id: str, message_id: str, body: Feedback) -> dict:
    workspace().genie.send_message_feedback(
        space_id(space), conversation_id, message_id, GenieFeedbackRating(body.rating.upper())
    )
    return {"rating": body.rating.upper()}


@app.get("/api/genie/{space}/conversations")
def conversations(space: str) -> list[dict]:
    found = workspace().genie.list_conversations(space_id(space), page_size=20).as_dict()
    return [conversation.conversation_summary(c) for c in found.get("conversations", [])]


@app.get("/api/genie/{space}/conversations/{conversation_id}")
def conversation_messages(space: str, conversation_id: str) -> list[dict]:
    found = workspace().genie.list_conversation_messages(
        space_id(space), conversation_id, page_size=100
    )
    messages = sorted(
        found.as_dict().get("messages", []), key=lambda m: m.get("created_timestamp", 0)
    )
    return [conversation.message_view(m) for m in messages]


@app.get("/api/views/{name}")
def view(name: str, request: Request, refresh: bool = False) -> dict:
    if name not in console.VIEWS:
        raise HTTPException(404, f"No view called {name!r}.")
    build, declared = console.VIEWS[name]
    parameters = []
    for param, kind in declared.items():
        value = request.query_params.get(param, "")
        if not value or (kind == "INT" and not value.isdigit()):
            raise HTTPException(400, f"The {name} view needs {param} ({kind.lower()}).")
        parameters.append(StatementParameterListItem(name=param, value=value, type=kind))
    return cached_query(build(settings().catalog), parameters, refresh)


@app.get("/api/digest/symbols")
def digest_symbols() -> list[dict]:
    ranges = cached_query(digest.ranges_query(settings().catalog), [])
    found = {(r[0], r[1]): (r[2], r[3]) for r in ranges["rows"]}
    return [
        {
            "symbol": name,
            "name": s.name,
            "venue": s.venue,
            "first_session": found.get((s.venue, name), (None, None))[0],
            "last_session": found.get((s.venue, name), (None, None))[1],
        }
        for name, s in digest.SYMBOLS.items()
    ]


def build_digest(symbol: str, as_of_text: str) -> dict:
    try:
        found = digest.symbol(symbol)
        as_of = digest.parse_as_of(as_of_text)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    catalog = settings().catalog
    common = [
        StatementParameterListItem(name="symbol", value=symbol, type="STRING"),
        StatementParameterListItem(
            name="as_of", value=digest.as_of_parameter(as_of), type="TIMESTAMP"
        ),
    ]
    window = [
        StatementParameterListItem(name="venue", value=found.venue, type="STRING"),
        StatementParameterListItem(name="lookback", value=str(digest.LOOKBACK_DAYS), type="INT"),
        StatementParameterListItem(name="ahead", value=str(digest.AHEAD_DAYS), type="INT"),
    ]
    bars = console.records(cached_query(digest.bars_query(catalog), common + window))
    snapshot = console.records(cached_query(digest.snapshot_query(catalog), common))
    filings = console.records(cached_query(digest.filings_query(catalog), common))
    return {
        "symbol": symbol,
        "name": found.name,
        "venue": found.venue,
        "as_of": as_of.isoformat().replace("+00:00", "Z"),
        "chat": settings().chat_endpoint is not None,
        "bars": [
            {"session_date": b["session_date"], "close": b["close"], "known": b["known"]}
            for b in bars
        ],
        "moves": digest.moves(bars),
        "snapshot": digest.snapshot_rows(snapshot),
        "filings": filings,
    }


@app.get("/api/digest")
def get_digest(symbol: str, as_of: str) -> dict:
    return build_digest(symbol, as_of)


_summaries: dict[tuple[str, str], dict] = {}


@app.post("/api/digest/summary")
def digest_summary(body: DigestRequest) -> dict:
    endpoint = settings().chat_endpoint
    if endpoint is None:
        raise HTTPException(503, "The app has no chat endpoint, so it shows the facts only.")
    found = build_digest(body.symbol, body.as_of)
    key = (found["symbol"], found["as_of"])
    if key not in _summaries:
        messages = digest.prompt(found)
        _summaries[key] = {
            "text": chat(endpoint, messages),
            "endpoint": endpoint,
            "facts": digest.facts(found),
        }
    return _summaries[key]


def chat(endpoint: str, messages: list[dict]) -> str:
    response = workspace().serving_endpoints.query(
        name=endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"]) for m in messages
        ],
        max_tokens=400,
        temperature=0.0,
    )
    if not response.choices or response.choices[0].message is None:
        raise HTTPException(502, f"{endpoint} returned no answer.")
    return response.choices[0].message.content or ""


@app.post("/api/ingest")
def start_ingest(body: IngestRequest) -> dict:
    found = settings()
    registry = cached_query(console.registry_query(found.catalog), [])
    registered = [row[0] for row in registry["rows"]]
    try:
        params = console.ingest_parameters(
            body.datasets, body.start, body.end, body.reload, body.max_files, registered
        )
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    run = workspace().jobs.run_now(job_id=found.ingest_job_id, job_parameters=params)
    host = workspace().config.host.rstrip("/")
    return {"run_id": run.run_id, "url": f"{host}/jobs/{found.ingest_job_id}/runs/{run.run_id}"}


@app.get("/api/ingest/runs")
def ingest_runs() -> list[dict]:
    runs = []
    for run in workspace().jobs.list_runs(job_id=settings().ingest_job_id, limit=10):
        status = run.status
        details = status.termination_details if status else None
        runs.append(
            {
                "run_id": run.run_id,
                "started": datetime.fromtimestamp(run.start_time / 1000, UTC).isoformat(),
                "state": status.state.value if status and status.state else "",
                "result": details.code.value if details and details.code else "",
                "parameters": {p.name: p.value or p.default for p in run.job_parameters or []},
            }
        )
    return runs


if DIST.exists():
    app.mount("/", StaticFiles(directory=DIST, html=True), name="web")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))

# ADR 0019: The console becomes a FastAPI and React app centred on Genie

Status: proposed (Stage 10). Builds on [ADR 0012](0012-console-app.md) and
[ADR 0014](0014-genie-spaces.md), and replaces ADR 0012's choice of Streamlit.

## Context

The Genie spaces answer well (both at full benchmark accuracy in dev and prod, ADR 0014), but the
only place to talk to them was the Databricks Genie page or the console's Ask tab. That tab is a
Streamlit form: it blocks until Genie finishes, shows nothing while Genie works, keeps one
conversation per browser session, and loses it on a reload. Stage 10 makes conversation with the
Genie the centre of the application. Its first step is the structure and a working interface; the
visual design comes after.

Free Edition allows three apps per account, and the dev and prod consoles take two. An app stops
24 hours after it starts, and only people who can sign in to the workspace can open it.

## Decisions

**The new app takes the console's place, not a third slot.** The bundle resource stays
`apps.console` (`pitlake-console-<target>`), so the app keeps its service principal, its URL, its
19 resources and the grants ADR 0012 and ADR 0014 set. The Streamlit `app/app.py` is deleted, and
every tab it had is in the new app.

**A FastAPI backend and a React frontend in one Databricks App.** `app/server.py` serves a JSON
API under `/api` and the built frontend at `/`. The frontend (`app/src`, React with TypeScript,
built by Vite) is built by the Apps runtime on deploy: when `package.json` is at the app root it
runs `npm install`, then `pip install -r requirements.txt`, then `npm run build`, then the
command, `python server.py`, which listens on `DATABRICKS_APP_PORT`. The build tools are under
`dependencies`, not `devDependencies`, because a production install skips the latter. `dist/`
and `node_modules/` are ignored by git and therefore by the bundle sync, so the workspace never
holds a stale build. The app stays a Databricks App because the platform already handles sign-in,
the service principal and its grants. The backend reads its settings from environment variables
and authenticates through the SDK's default chain, so the same code runs on a laptop
(`make console-api`) and could later run on another host.

**Genie questions are asynchronous: the browser polls.** `POST /api/genie/{space}/messages`
starts a conversation, or continues one when a conversation id is given, and returns at once with
the message id. The browser then polls `GET .../messages/{id}` (1 s, growing to 5 s, for at most
10 minutes) and shows Genie's stage as it goes: reading the space, writing the query, waiting for
the warehouse, running the query. A request never waits on Genie, so no proxy timeout applies. A
finished message shows Genie's text, each query's title, description, SQL and result, Genie's
suggested follow-up questions, and buttons that send feedback to the space. Earlier conversations
are listed per space and can be reopened, since Genie keeps them. A rate-limit error from the free
tier (about five questions a minute) becomes HTTP 429 and a message to wait.

**The data tabs read through one endpoint, typed by the warehouse's schema.** `console.VIEWS`
names each query with the parameters it takes, and `GET /api/views/{name}` runs it through the
SQL Statement Execution API, which removes the SQL connector and pandas from the app. Values come
back as strings; `console.result_table` converts them with the column types in the result's
manifest (integers, decimals, booleans, and JSON for maps, arrays and structs), and returns a
non-finite number as null so the response stays valid JSON. Genie's query results go through the
same function. Results are cached for five minutes, as before, and a Refresh button bypasses the
cache. The backtest list reads its headline metrics from the recorded map in SQL.

**The grants and the queries are unchanged.** The app reads the same 15 tables and has the same
resources, so ADR 0012's and ADR 0014's tests still hold. New tests check that every view declares
exactly the parameters its query uses, that the starter questions are the spaces' deployed sample
questions, and the API: questions, follow-ups, unknown spaces, blank questions, rate limits, view
parameters, the cache and the ingest form, against a fake workspace.

## Consequences

- Every app deploy runs `npm install` and the frontend build inside the app. CI builds it too
  (job `web`), so a type error fails CI before it fails a deploy.
- The conversations belong to the app's service principal: everyone who opens the app sees the
  same list of earlier conversations. Asking on behalf of each viewer needs user authorization
  for Apps, which this step does not use.
- Charts were dropped for now; the data tabs show tables. The visual design is the next step of
  Stage 10.
- Locally, `make console-api TARGET=dev` runs the backend as the developer, with the ids read from
  `databricks bundle summary`, and `make console-web` serves the frontend with hot reload and a
  proxy to it.

# ADR 0033: The console publishes attribution, sufficiency and data value through a results schema

Status: proposed (counterfactual information, console). Builds on
[ADR 0012](0012-console-app.md), [ADR 0019](0019-console-web-app.md),
[ADR 0025](0025-attribution.md), [ADR 0026](0026-sufficiency.md) and
[ADR 0030](0030-data-value.md).

## Context

The counterfactual specification gives the console one role in the programme: it publishes the
attribution and sufficiency results. Until now they were read from the jobs' printed output and
from SQL by hand.

ADR 0012 grants the app SELECT on exactly the tables it reads, one app resource per table. The
app already declares 20 resources: the warehouse, the ingest job, two Genie spaces, the chat
endpoint and 15 tables. Twenty is the platform's limit per app, and a test checks it. An app
resource of type `uc_securable` can name a table, volume, function or connection, but not a
schema. So no further table can be granted that way.

## Decisions

**A `results` schema holds views of what the console publishes.** For each published control
table there is a view `results.<table>` of the same name that selects every column of
`control.<table>` and nothing else: `attribution_points`, `attributions`, `sufficient_sets` and
`source_values`. The bundle declares the schema (`resources/storage.yml`) with one grant,
`USE_SCHEMA` and `SELECT` to the app's service principal
(`${resources.apps.console.service_principal_client_id}`). A view in Unity Catalog reads its
tables with its owner's rights, so the app reads these four tables through the views and still
has no right on the rest of `control`. The schema grant costs no app resource. Publishing
another table is one more entry in `PUBLISHED` in `pitlake.tables`.

**The smoke job and every counterfactual job create the views.** They are part of
`counterfactual_ddls`, after the tables they read, so `make smoke` after a deploy creates them
without running an experiment, and a change of a table's columns reaches its view on the next
run of either. `results` is a layer in `pitlake.config`, so the smoke check fails when the schema
is missing.

**The page summarises; the views do not.** `app/console.py` reads the views and does the same
reductions the jobs print:

| Section | Reads | What it shows |
|---|---|---|
| Which information caused each decision | `results.attribution_points`, `results.attributions` | One row per decision: the latest record of its real-world decision, how many single-item worlds were decided, screened and causing, whether the null world changed nothing, and how many worlds equal the full rebuild |
| Items that change a decision | `results.attributions` | For the decision chosen, each item whose removal changed a settled part, with the parts, effects and p-values, largest effect first |
| The least information that reproduces each decision | `results.sufficient_sets`, `results.attribution_points` | One row per decision: the candidates, the size of the minimal set by kind, the tests it took, and whether it was confirmed from the set alone and is coherent; choosing one lists its items |
| Which sources change decisions | `results.source_values` | Per backtest, the latest valuation of each decider: each source's verdict, the decisions and parts it changed, and what attribution of its items one at a time found |

The latest record wins wherever a job wrote one more than once: a decision's newest
`attribution_points` row, and per decision and withheld set the newest valid `attributions` row,
as the attribution job reloads them. The null world passes as in `attribution.null_passed`: valid,
every outcome `same`, nothing caused and no p-value below 0.05; a test keeps the console's
constants equal to the job's.

## Consequences

- On a new catalog the schema's grant names an app that does not exist yet, so the first deploy
  also fails on it, with the console app (deploy runbook B1); C1 deploys both.
- The deploy creates the schema, not the views: until the smoke job or a counterfactual job has
  run after it, the page fails with a missing table.
- The page shows what the tables hold, finished or not: a decision with worlds left shows the
  worlds done so far. Whether an experiment is complete is still what its job prints and
  `make status` reads.
- Tests: `tests/test_console.py` (the schema's grant and the published views agree with the
  queries, each view selects every column of its table, the null-world rule, the latest-record
  rules) and `tests/test_information.py` (22 tables and 4 views).

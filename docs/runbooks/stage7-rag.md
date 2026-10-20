# Stage 7 runbook: point-in-time RAG over filings, vector search and the fine-tuned retriever

The ingest job now also lands the text of every 10-K and 10-Q, and the transform cleans it into
`silver.edgar_filing_documents`. The `rag` job runs on demand. It cuts the committed text into
passages, embeds the new ones, syncs the AI Search index, fine-tunes the query adapter, and
evaluates base against tuned retrieval. The `ask` job answers one question as of an instant
([ADR 0010](../adr/0010-point-in-time-retrieval-and-fine-tuning.md)).

Follow the parts in order. If you see something other than what a step says, stop and keep the
full output.

---

## Part 0: workspace

### 0.1 Sign in again

```bash
databricks auth login --profile pitlake
```

### 0.2 Check the model endpoints

```bash
databricks serving-endpoints list -o json | jq -r '.[].name' | sort
```

✅ The list contains `databricks-gte-large-en` and a chat model. If the chat model is not
`databricks-meta-llama-3-3-70b-instruct`, set `chat_endpoint` in `databricks.yml` to one that is
listed before you deploy.

---

## Part A: dev

### A1. Test and deploy

```bash
make check
make validate
make deploy
```

✅ **Jobs & Pipelines** shows `pitlake-rag-dev` and `pitlake-ask-dev`, and **Experiments** shows
`pitlake-rag-dev`.

### A2. Land the filing documents

```bash
databricks bundle run ingest -t dev --params max_files=0
```

✅ `TERMINATED SUCCESS`, with a `collect_feed` iteration for `edgar.filing_documents`. In the SQL
editor:

```sql
SELECT _symbol, count(*) AS documents, min(_period_start) AS first, max(_period_start) AS last,
       sum(length(document)) AS characters
FROM pitlake_dev.bronze.edgar_filing_documents GROUP BY _symbol;
```

✅ AAPL and MSFT have about 5 documents each, from early 2024 to early 2025, with no row that has
`_corrupt_record`.

### A3. Clean them

```bash
make transform
```

✅ `TERMINATED SUCCESS`. The EDGAR group prints `edgar.filing_documents transformed ... files`.

```sql
SELECT symbol, accession_number, form, report_date, knowable_at, html_chars
FROM pitlake_dev.silver.edgar_filing_documents ORDER BY knowable_at;
```

✅ Every row has a form, a report date and a `knowable_at`, and `silver.quarantine` has no
`edgar_filing_documents` row.

### A4. Build the index and fine-tune

```bash
make rag
```

The first run creates the AI Search endpoint `pitlake`, which can take 10 to 20 minutes.

✅ `TERMINATED SUCCESS`, and the tasks print:

- `chunk`: `OK: <N> chunks of 10 filings`. N is about 1,000 to 2,000.
- `embed`: `OK: <N> chunks embedded ..., 0 left`.
- `index`: `OK: pitlake_dev.gold.chunk_embeddings_index serves <N> chunks`.
- `tune`:
  - `questions per split: {'train': ..., 'validation': ..., 'test': ...}`, every split above 0.
  - One line per retriever and split, each with `future 0`. The `no-pit` lines may show future
    passages: that is what the filter prevents.
  - `vector index vs exact search: {"recall_at_10": ..., "invisible_before_acceptance_cases": 0.0, ...}`.
  - `answers through ...: {...}`.
  - `OK: recorded ... retrieval evaluations`.

Copy the **run ID** from the Run URL. With dev's handful of filings the tuned numbers are noisy:
this step proves the plumbing.

### A5. Replay the evaluation

```bash
databricks bundle run rag -t dev --params replay=<run ID from A4>
```

✅ `TERMINATED SUCCESS`. Only the `replay` task runs, the build tasks show as excluded, every line
starts with `IDENTICAL`, and the run ends `OK: reproduced ... retrieval evaluations`.

### A6. Ask a question

`bundle run --params` splits on commas, so ask without commas here:

```bash
databricks bundle run ask -t dev --params \
  question="What were Apple's total net sales for the quarter ended June 29 2024?",as_of=2024-08-02T12:00:00Z,symbol=AAPL
```

✅ An answer that quotes 85,777 (million) with a citation, and a list of sources that are all
filings accepted on or before 2024-08-02. Ask the same question with `as_of=2024-08-01T12:00:00Z`:
the 10-Q is not yet knowable, so the answer must say the filings do not tell.

For a question with commas, use the job ID from **Jobs & Pipelines**:

```bash
databricks jobs run-now <job ID> --json '{"job_parameters": {"question": "In fiscal 2024, how did Apple describe its Services growth?", "as_of": "2024-11-15T00:00:00Z", "symbol": "AAPL"}}'
```

---

## Part B: prod

### B1. Deploy and land the documents

```bash
make deploy-prod
databricks bundle run ingest -t prod --params max_files=0
```

✅ `TERMINATED SUCCESS`, with about 90 to 100 documents for AAPL, MSFT and NVDA since 2019 in
`pitlake_prod.bronze.edgar_filing_documents`. The ingest commit starts the transform. Wait for it
to finish with `TERMINATED SUCCESS`.

### B2. Build, fine-tune and evaluate

```bash
databricks bundle run rag -t prod
```

✅ As in A4, with about 15,000 to 25,000 chunks. The test split is the filings accepted since
2025-01-01. Copy the **run ID**. If the embed task hits the daily quota, run it again with
`--params max_chunks=5000` until it prints `0 left`.

---

## Part C: exit test

### C1. No leak, and the index agrees with exact Euclidean search

```sql
SELECT retriever, split, point_in_time, metrics['future_chunks'] AS future,
       metrics['hit_at_1'] AS hit1, metrics['hit_at_5'] AS hit5, metrics['mrr_at_10'] AS mrr10,
       metrics['questions'] AS questions
FROM pitlake_prod.control.retrieval_runs
WHERE retrieval_run_id = '<run ID from B2>' AND retriever IN ('base', 'tuned')
ORDER BY split, point_in_time DESC, retriever;

SELECT metrics FROM pitlake_prod.control.retrieval_runs
WHERE retrieval_run_id = '<run ID from B2>' AND retriever IN ('vector_index', 'rag_answers');
```

✅ The criteria are:

- `future = 0` on every row where `point_in_time` is true.
- `invisible_before_acceptance_cases = 0`.
- `recall_at_10` is at least 0.95.
- `score_max_abs_error` is under 0.001.
- On the test split with the filter, the tuned retriever's `mrr10` is above the base retriever's.

### C2. Replay

```bash
databricks bundle run rag -t prod --params replay=<run ID from B2>
```

✅ Every line is `IDENTICAL`, and `control.retrieval_runs` has rows with
`replay_of = '<run ID from B2>'` and `reproduced = true`.

The numbers from C1 and C2 go into ADR 0010 under "Exit test".

# ADR 0010: Point-in-time retrieval over filings, vector search and a fine-tuned retriever

Status: accepted (Stage 7). Builds on [ADR 0008](0008-gold-and-point-in-time-access.md) and
[ADR 0009](0009-research-and-reproducible-backtests.md).

## Context

Spec section 10 gives Stage 7 to operations, the application, monitoring and performance work. On
2026-10-20 the owner moved that work to a Stage 8 and gave Stage 7 a new scope: answer questions
about the companies from their filings with retrieval-augmented generation (RAG), using vector
embeddings, Euclidean distance, Databricks Vector Search, and fine-tuning in the sense of the
Databricks guide [A Practical Guide to LLM Fine Tuning](https://www.databricks.com/blog/llm-fine-tuning).

The project's one rule still applies: nothing may be read before it was knowable. Text has the
same problem as numbers. Each 10-K repeats the prior year as comparatives, sometimes restated, so a
search over every filing answers a question about 2023 with a number published in 2025. The
retrieval layer therefore has to filter on knowable_at the way `pitlake.pit.as_of` does.

Free Edition sets the limits. It allows one AI Search endpoint (AI Search was formerly Vector
Search) with one unit and no Direct Vector Access. It has no GPU serving and no custom models on
GPU. Serverless GPU training is a preview that needs LinkedIn verification. Outbound internet is
limited to trusted domains.

## Decisions

**The corpus is the text of every 10-K and 10-Q.** A new collector dataset, `edgar.filing_documents`,
lands the primary HTML document of each periodic XBRL report (10-K, 10-Q and their amendments) in a
filing's window, one file per filing, exactly as received from `www.sec.gov/Archives`. As with the
other API sources, validation replaces the checksum: the body must be HTML, end with `</html>`, and
be at least 10 kB. Some older filings, such as Apple's 10-Q of 2019-01-30, are served with the
EDGAR SGML wrapper's closing `</TEXT>` and `</DOCUMENT>` after `</html>`, so those two tags may
follow it. Bronze reads each file whole (`file_format = "document"`, one row per file).
Prod collects AAPL, MSFT and NVDA from 2019-01-01 and dev collects AAPL and MSFT from 2024-01-01 to
2025-03-31. Partitions come from the same `filings.recent` list as the `edgar.filings` snapshots, so
every document's accession is in a snapshot. That list holds only a company's latest 1,000 or so filings, mostly
insider Form 4s, so prod's corpus starts at the window for AAPL (2019-01-30) but only at the 10-Qs
of 2020-10-27 for MSFT and 2020-11-18 for NVDA: 79 documents. The older filings are in the
`filings.files` pages, which neither dataset reads.

**A document is knowable at its acceptance.** `silver.edgar_filing_documents` looks up the
acceptance time, form, filing date and period of the report in `silver.edgar_filings`. Knowable_at
is the acceptance time. A document whose filing is missing has no knowable_at, and
`knowable_at_present` halts the transform. The plausibility rule accepts a NULL time on purpose:
otherwise it would drop the row first and the halt could never fire. A document of under 10,000
characters is dropped (`document_has_text`).

**Passages are cut in Python, outside the transform.** `pitlake.documents` turns HTML into lines
with the standard library parser. It skips `ix:header` (the hidden inline XBRL), scripts, styles and
the head. It writes each table row as its cells joined by ` | `, dropping currency-sign cells and
reattaching `)` and `%`, so `(142 | )` becomes `(142)`. Lines are grouped into passages of at most
2,000 characters that overlap by up to 300. Each passage records the latest `Item`/`PART` heading
as its section. Its ID is a SHA-256 of the accession, index and text, so unchanged passages keep
their IDs and are not embedded again. Every passage also gets a context line naming the company,
form, period of the report, filing date and section, and the context is embedded with the text. This
puts the period into the vector, which a table of numbers alone would not. The RAG job, not the
transform, builds `gold.filing_chunks` from the committed Silver version: embedding costs endpoint
calls, and the transform runs after every ingest.

**Embeddings come from the Foundation Model API.** `databricks-gte-large-en` (1,024 dimensions) is
called through `serving_endpoints.query`, with no new libraries. A local sentence-transformer would
need PyTorch and a download from Hugging Face, which the restricted egress may block. Vectors are
scaled to unit length and appended to `gold.chunk_embeddings`, which has change data feed (needed by
the index) and the same 365-day retention as the rest of Gold. Only new passage IDs are embedded.
`max_chunks` spreads a large backfill over several runs when the daily quota is a concern. Requests
carry 8 texts each because Free Edition counts every text against the workspace QPS limit for the
endpoint: on 2026-10-21, batches of up to 12 passages were accepted and batches of 16 or more were
refused with `REQUEST_LIMIT_EXCEEDED` every time, even after a pause. The SDK retries a refused
request for five minutes and then fails the task, so a batch over the limit can never succeed.
Sequential requests of 8 take about 3 seconds each, which keeps the rate well below the limit.

**Distance is Euclidean.** AI Search uses HNSW with L2 distance, and its score is
`1 / (1 + d²)`. `pitlake.retrieval` computes the same squared Euclidean distance exactly with numpy,
as `‖q‖² + ‖d‖² − 2 q·d`. For unit-length passages, `‖q‖²` is the same for every passage of a query,
so the L2 ranking equals the cosine ranking even when the adapted query is not unit length. Exact
search is the reference and the reproducible path. The index is the serving path. The job measures
the index's recall@10 against exact search and checks its score against `1 / (1 + d²)`.

**Retrieval is point-in-time.** Every passage carries `knowable_at_us`, its filing's acceptance time
in microseconds. Both paths filter `knowable_at_us <= as_of` before ranking: the numpy mask and the
index filter `{"knowable_at_us <=": as_of}`. Filtering after ranking would let the future take
places in the top ten. `query_index` refuses any result the filter should have excluded. Questions
also filter on the company they name.

**The retriever is fine-tuned, not the LLM.** On Free Edition a fine-tuned LLM could not be served,
because there is no GPU serving and no custom models on GPU. Training one also needs the serverless
GPU preview. The guide's method still fits the part of RAG that decides what the model reads.
This is parameter-efficient fine-tuning in the LoRA form: the base embedding model is frozen, and a
trainable low-rank update `ΔW = AB` (rank 16) is applied to the query embedding, `q' = q + qAB`.
`B` starts at zero, so training starts exactly from the base model, as LoRA does. Passages are
untouched, so retraining the adapter never re-embeds the corpus or rebuilds the index. The loss is a
softmax over negative squared Euclidean distances, with temperature 0.05, between the adapted
question and three kinds of passage, all visible at the question's instant:

- its relevant passages;
- 32 hard negatives, the nearest wrong passages, recomputed every 10 epochs;
- 32 random negatives.

It is minimized with Adam (learning rate 0.0001, batches of 64). The guide's evaluation advice is
followed: the metric (MRR@10 on the validation split) is fixed before training, checked after every
epoch, and training stops after five epochs without improvement, keeping the best adapter. The
code is numpy, seeded and tested on every change without a cluster.

**Training data is generated from what Gold already knows.** `pitlake.questions` builds questions
from `gold.fundamentals` without hand labelling:

- **Which value:** for every company, metric, duration and period, the filing that first reported
  it (later restatements are ignored).
- **When it is asked:** at that filing's acceptance.
- **Its label:** the passages of that filing that print the value as filed, whole numbers only
  (`85,777` for 85,777,000,000 in a table in millions, and two decimals for EPS).
- **Dropped questions:** those whose value cannot be found in the text, counted by reason.

Questions are split by time. Those answerable before `train_until` train, those up to `test_from`
validate, and later ones test. Each split has its own phrasings, so validation, like test,
measures questions worded in a way training never saw. Because a
question is asked at the acceptance of the first filing to report the value, later filings that
repeat it as a comparative are neither relevant nor visible. The point-in-time filter is what makes
the task well-posed.

**Every evaluation is recorded twice.** `control.retrieval_runs` records each retriever (`base` or
`tuned`) on each split, with its hit@1/5/10, MRR@10, the count of future passages it returned, a
rankings hash, the table versions, the adapter's SHA-256, and the MLflow run. The base and tuned
retrievers are also scored on the test split without the point-in-time filter, which shows how many
future passages a naive search would have used. A `vector_index` row records the index check. A
`rag_answers` row records, for `answer_sample` test questions, how often the chat model's answer
contains the reported value and cites a relevant excerpt, with each retriever. MLflow
(`/Users/<user>/pitlake-rag-<target>`) holds the parameters, the training curve, every metric, and
`adapter.npz`.

**A replay reproduces an evaluation.** With `replay` set, the job reads the recorded versions of
`gold.chunk_embeddings` and `gold.rag_questions`, downloads the adapter and verifies its hash, and
recomputes every base and tuned ranking. It fails unless every metric and rankings hash is
identical. Question embeddings are stored for this, because an endpoint may be updated under the
same name. The index and the chat model are services without versions, so their rows are not
replayed.

**Answers cite and are recorded.** The `ask` job embeds a question, applies the latest tuned
adapter, and retrieves five passages knowable at `as_of` (default now) from the index. A chat
endpoint (`databricks-meta-llama-3-3-70b-instruct` by default, temperature 0) then answers. The
system prompt states the instant and asks for figures exactly as printed, with `[n]` citations.
Each answer, with what it retrieved, goes to `control.rag_answers` and MLflow.

## Consequences

- The retrieval is point-in-time; the generator is not. A model trained in 2025 may remember
  Apple's 2024 results and use them when asked about 2023, whatever the prompt says. PITLake cannot
  close that leak. For research, use the retrieved passages and their citations, which are
  guaranteed knowable, not the free text of the answer.
- The index serves the latest synced table, not a version. Reproducible results come from exact
  search over a recorded version. Exact search over the whole corpus takes milliseconds per
  question with numpy.
- Free Edition allows one AI Search endpoint, so dev and prod share `pitlake`, each with its own
  index (`<catalog>.gold.chunk_embeddings_index`).
- Switching the embedding model means dropping the index and `gold.chunk_embeddings`. The embed
  task refuses to mix models.
- A question with a comma cannot be passed through `bundle run --params`, which splits on commas.
  The runbook uses `databricks jobs run-now --json` for those.
- If LinkedIn verification grants serverless GPU, a LoRA fine-tune of a small open LLM on these
  questions is a natural follow-up. It would be evaluated by the same `rag_answers` check, and
  without GPU serving it could only answer from within a job.

- The first prod run (269968689926270) checked validation every 10 epochs at learning rate 0.001,
  and the tuned retriever lost to base on test (MRR@10 0.288 against 0.438, hit@1 0.115 against
  0.332). Validation MRR@10, checked after every epoch, peaks within 1 to 10 epochs and then falls
  as the adapter memorizes the training labels: its top hit moved to a chunk of an older filing,
  where training labels live and which the point-in-time filter keeps visible, for 93 of 119
  validation questions against 31 for base. The first check already came after the peak. The fix
  was chosen on the training and validation splits alone, without looking at test: checking every
  epoch at learning rate 0.0001 brings validation MRR@10 from 0.448 to 0.73-0.77, also when
  validation is worded in phrasings training never saw (0.482 to 0.755).

## Exit test

Stage 7 is complete when, in prod:

1. No evaluation leaks. Every point-in-time row in `control.retrieval_runs` has `future_chunks = 0`,
   and the index returns no passage of a filing one microsecond before its acceptance
   (`invisible_before_acceptance_cases = 0`).
2. The index agrees with Euclidean distance. Its recall@10 against exact search is at least 0.95,
   and its score matches `1 / (1 + d²)` to within 0.001.
3. Fine-tuning is measured on filings it never saw. Base and tuned hit@k and MRR@10 on the test
   split are published here, and the tuned retriever has the higher MRR@10.
4. A replay of the evaluation returns identical results.

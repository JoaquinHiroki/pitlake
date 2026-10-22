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
- LinkedIn verification granted serverless GPU, so the generator was fine-tuned too. See
  "Generator" below.

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

### Result (prod, 2026-10-21)

The corpus is 79 filings in 8,849 chunks, and `gold.fundamentals` version 12 gave 800 questions:
473 train (accepted before 2024-01-01), 119 validate, and 208 test (accepted since 2025-01-01).
Retrieval run 366086448172212 read `gold.chunk_embeddings` version 11 and `gold.rag_questions`
version 2, and stopped training the rank-16 adapter after 12 epochs.

| Retriever, test split | hit@1 | hit@5 | MRR@10 | future passages |
|---|---|---|---|---|
| base, point-in-time | 0.332 | 0.587 | 0.438 | 0 |
| tuned, point-in-time | 0.572 | 0.851 | 0.680 | 0 |
| base, no filter | 0.303 | 0.524 | 0.403 | 416 |
| tuned, no filter | 0.519 | 0.808 | 0.638 | 394 |

On validation, MRR@10 went from 0.482 to 0.755, and on train from 0.507 to 0.779.

1. Every point-in-time row has `future_chunks = 0`, and the index returned no passage of a filing
   before its acceptance in 100 test questions (`invisible_before_acceptance_cases = 0`).
2. The index's recall@10 against exact search was 1.0 for every one of those questions, and its
   score differed from `1 / (1 + d²)` by at most 9.7e-8.
3. The tuned retriever has the higher test MRR@10, 0.680 against 0.438. With its excerpts, the chat
   model's answer contained the reported value for 16 of 20 test questions against 13 with base,
   and cited a relevant excerpt for 17 against 14.
4. Replay run 219931683824982 reproduced all 8 evaluations: each has `reproduced = true` and the
   same rankings hash and metrics as the original.

Stage 7 is complete.

## Generator

Added on 2026-10-22, once LinkedIn verification granted serverless GPU (AI Runtime, one A10G with
22 GB).

**Llama 3.2 3B Instruct is fine-tuned with LoRA to answer from the tuned retriever's excerpts.**
The `pitlake-generator-<target>` job has three tasks:

- **prepare** (CPU) takes the latest tuned retrieval run, ranks every question's five excerpts
  exactly as that run would (same table versions and adapter, point in time, with a check that
  none comes from the future), and writes each prompt with its target to the `gold.staging` volume.
- **train** (GPU) fits rank-16 LoRA on every linear layer of the frozen base model and keeps the
  epoch with the lowest validation loss. Then it answers the test split with and without the
  adapter.
- **score** (CPU) asks the chat endpoint the same test prompts and records all three in
  `control.retrieval_runs` (`retriever = 'generator'`). It logs the adapter to MLflow.

The base model comes from `system.ai`, so nothing is downloaded from the internet. Base, LoRA and
reference answer identical prompts, so only the generator differs.

**The targets are built, not written by a larger model.** When an excerpt from the filing that
first reported the value prints it, the target is `<Company> reported <label> of <value> <period>
[n]`, citing the first such excerpt. Otherwise it is the abstention the system prompt asks for:
`The filings available on <instant> do not say.` So the model learns both when to cite and when to
abstain, and it learns them only from passages that were knowable at the time. The targets are
deterministic, which keeps the training data reproducible.

**A third of the training and validation questions also come without their answer.** The tuned
retriever was trained on the training questions, so it finds their answer almost every time. The
first dev run (529346727847839) had 40 answerable training examples and no abstention, and the
fine-tuned model learned to always answer. It abstained on 0 of the 8 unanswerable test questions
(base 1, the chat endpoint 7), and answered them with a figure from an older period, which is the
leak this system exists to prevent. Accuracy still rose from 0.707 to 0.756 and relevant citations
from 0.659 to 0.732.

Now every question whose SHA-256 is divisible by 3 gets a second example. Its excerpts are the five
best-ranked visible passages that do not print the value, and its target is the abstention. This is
how a retrieval miss looks. The test split is left as retrieval made it.
With them, the second dev run (787970490856598) abstained on 7 of the 8 unanswerable questions,
as the chat endpoint does. It also abstained on 4 of the 33 answerable ones, all Microsoft 10-Q
passages where the value comes after a table of contents. Its share of correct behavior went from
0.756 in the first run to 0.829 (base 0.732, chat endpoint 0.976).

**The prompt states the question's date, never today's.** The Llama 3 chat template writes
`Today Date` into the system prompt and defaults it to the day the code runs. That would tell the
model the future, and it would make the training data depend on the run date. The template is
given the as-of date of each question instead.

**Scoring** reuses the `rag_answers` check. `accuracy` counts answers that contain the reported
value, and `cited_relevant` counts answers that cite an excerpt from the right filing. `correct`
counts the behavior wanted: the value when an excerpt prints it, and the abstention when none
does. It is the headline measure, because `accuracy` gives no credit for a correct abstention.
Two more measures cover abstention: `abstained_when_unanswerable` is the share of questions without a
relevant excerpt where the model said the filings do not say, and `abstained_when_answerable` is
the same share where it should have answered.

### Result (prod, 2026-10-22)

Generator run 574414218096058 (commit 19317fd) used the excerpts of tuned retrieval run
366086448172212. Prepare wrote 629 training examples (435 answers and 194 abstentions), 151
validation and 208 test examples, and the test split has 177 answerable and 31 unanswerable
questions. Validation loss went from 0.316 to 0.0139 after epoch 1 and 0.0044 after epoch 2, then
rose to 0.0068 after epoch 3, so epoch 2's adapter was kept. The run took 102 minutes, 94 of them
on the GPU.

| Generator, test split | correct | accuracy | cited relevant | abstained, unanswerable | abstained, answerable |
|---|---|---|---|---|---|
| Llama 3.2 3B, base | 0.827 | 0.803 | 0.731 | 0.161 | 0.000 |
| Llama 3.2 3B, LoRA | 0.981 | 0.851 | 0.846 | 0.871 | 0.000 |
| Llama 3.3 70B, chat endpoint | 0.971 | 0.851 | 0.784 | 0.871 | 0.000 |

The fine-tuned 3B gives the value for every answerable question (accuracy equals the answerable
share, 0.851) and abstains on 27 of the 31 unanswerable ones, as often as the 70B endpoint. It
cites the right filing more often than the endpoint (0.846 against 0.784). With prod's 629
training examples, the over-abstention seen in the second dev run did not appear, so the epoch is
still chosen by validation loss.

Consequences:

- Free Edition cannot serve the fine-tuned model, so it answers only within the job. The
  `ask` job keeps using the chat endpoint.
- Generation is greedy, but GPU kernels are not bit-for-bit deterministic. A rerun can therefore
  differ in a few answers. The predictions file's SHA-256 is recorded as the run's
  `rankings_hash`, and nothing is replayed against it.
- `system.ai` creates a model's versions the first time anyone asks for them, and that first
  listing comes back empty. The model lookup retries for up to two minutes.
- `ai_runtime_task` takes no parameters. The GPU task finds its job run through its MLflow run's
  `mlflow.databricks.jobRunID` tag, its catalog through the job's `catalog` tag, and its settings
  through the job parameters.

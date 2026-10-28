# ADR 0017: A fine-tuned reranker orders the retriever's point-in-time candidates

Status: proposed (Stage 9). Builds on [ADR 0010](0010-point-in-time-retrieval-and-fine-tuning.md)
and [ADR 0016](0016-independent-as-of-check.md).

## Context

The tuned retriever of ADR 0010 finds a relevant passage in its top five for 85.1% of the prod
test questions, but ranks one first for only 57.2% (MRR@10 0.680). The passages are there; their
order is wrong. The generator reads the top five, so the order decides what it can cite, and a
better first passage is what an analyst looking at the sources sees first.

A bi-encoder such as the retriever embeds the question and the passage separately and compares
two vectors. A cross-encoder reads them together and can check that the passage states this
company's figure for this period, which is what the relevance labels mean. It costs one model call
per passage, so it only reorders a short list.

Serverless GPU is available since the generator (ADR 0010, "Generator"), and `system.ai` holds
Llama 3.2 1B Instruct as well as 3B.

## Decisions

**The tuned retriever's top 20 are reranked; nothing outside them can be.** `prepare` ranks every
question with the tuned retriever exactly as the generator's `prepare` does (the same function,
`tuned_rankings`): the point-in-time and company filters apply before ranking, and any candidate
knowable after its question fails the task. The reranker only changes the order of passages the
retriever was allowed to see, so it cannot introduce a leak, and the share of questions with a
relevant passage among the 20 (`hit_at_20`) is the ceiling it is measured against.

**Keyword and hybrid candidates are measured beside them, and not yet used.** If the relevant
passage is not in the dense top 20, no reranking can recover it. Filing questions name exact
line items and dates ("total net sales", "June 29, 2024"), which word matching finds and an
embedding can blur, so `prepare` also ranks every question with BM25 (k1 1.2, b 0.75) and with a
hybrid, the reciprocal rank fusion (k = 60) of the dense and BM25 top 100, and records hit@1,
hit@5, MRR@10 and hit@20 of `dense`, `bm25` and `hybrid` per split in the manifest and, for the
test split, in MLflow. Both apply the same point-in-time and company filter as the dense search,
and so do BM25's statistics: document frequencies and the average passage length are counted over
the passages visible at the question's instant only, because a word's rarity among filings not
yet published is information from the future. A filing accepted later therefore changes no
earlier score, which a test checks. The reranker keeps the dense candidates
(`candidate_source = dense`), so its first result answers the planned question; whether the hybrid
should supply them is decided from these numbers.

**Llama 3.2 1B Instruct judges each passage, fine-tuned with LoRA.** The prompt gives the passage
and the question, states the question's instant (Llama's template writes today's date otherwise,
as for the generator) and asks whether the passage states the figure. The score is
`logsumexp(logit(yes), logit(Yes)) − logsumexp(logit(no), logit(No))` at the first answer token,
so the untrained model can be scored the same way. 1B rather than 3B: it reads 20 passages per
question, about 20,000 prompts per evaluation of all splits, and a judgement needs less than a
written answer.

**Training is listwise over hard negatives.** Each step takes one training question with a relevant
candidate: one relevant passage and seven non-relevant ones from its own top 20, which are the
retriever's mistakes and therefore hard. The loss is the cross-entropy of a softmax over the eight
scores with the relevant one as the target (RankLLaMA's objective). Gradients accumulate over four
questions; AdamW at 1e-4; LoRA rank 16 on every linear layer; passages cut to 768 tokens. Prompts
are left-padded in batches, with positions counted from each prompt's first token so padding does
not shift them.

**The epoch is chosen by validation MRR@10, fixed before training.** That is the retriever's own
metric (ADR 0010), computed by the same `retrieval.metrics`: only the top ten count, ties keep the
tuned order. Epoch 0 is the untrained model, so a fine-tune that does not help is not kept. Two
epochs by default, patience one.

**Three orders are compared on the same candidates of the test split.** `tuned` (the retriever's
order), `base` (the untrained 1B) and `lora`. `score` checks that every reranking is a permutation
of its question's candidates, recomputes the future-passage count, and, when the chat endpoint is
set, asks it each test question twice, with the top five of `tuned` and of `lora`, scoring the
answers with the generator's `score`. That measures whether the better order reaches the answers.

**Results are recorded as two retrievers in `control.retrieval_runs`.** `rerank_base` and
`rerank_lora`, with the job run as `retrieval_run_id`, the tuned baseline's metrics prefixed
`tuned_`, the SHA-256 of `rerankings.tsv` as `rankings_hash`, and the adapter's SHA-256. No `tuned`
row is written, because `ask` and the generator take the latest `tuned` row as their retriever.

**`tools/rankcheck` recomputes the result from Gold.** As `pitcheck` does for backtests,
`make rankcheck TARGET=<target> RUN=<run ID>` exports, read-only, the run's recorded metrics and
table versions, the test questions of `gold.rag_questions` and every chunk's company and
`knowable_at_us` from `gold.chunk_embeddings` at those versions, and the run's `rerankings.tsv`. The
OCaml program checks the file's hash against the record, then recomputes every metric from the
relevance in Gold rather than from the candidates file the Python wrote, and fails if any ranked
passage was knowable after its question or belongs to another company. The rerankings are TSV so
the tool needs no JSON parser.

## Consequences

- Free Edition cannot serve the adapter, as for the generator, so the reranker improves the
  evaluated pipeline in the job. `ask` and the console keep the tuned retriever's order.
- GPU kernels are not bit-for-bit deterministic, so a rerun can order a few near-tied passages
  differently. What is reproducible is the evaluation of a recorded reranking: `rankcheck` rebuilds
  its metrics exactly.
- The run's folder is in the Gold staging volume, which the operations job clears after seven
  days; `rankcheck` must run within that week. `score` also logs `rerankings.tsv` to MLflow.
- BM25 here is plain Python and numpy over about 9,000 passages: building the index and ranking
  800 questions takes seconds on serverless CPU, and nothing is served or indexed elsewhere.
- Questions whose relevant passage is not among the 20 stay wrong whatever the reranker does;
  `hit_at_20` reports how many.

## Exit test

On the prod test split, `rerank_lora` has a higher MRR@10 and hit@1 than `tuned` on the same
candidates, with `future_chunks = 0`, and `make rankcheck` prints IDENTICAL for the run.

# Stage 9 runbook: fine-tuned reranker

Reranks the tuned retriever's point-in-time top 20 passages with Llama 3.2 1B Instruct fine-tuned
with LoRA on serverless GPU, and checks the result with an independent OCaml program
([ADR 0017](../adr/0017-fine-tuned-reranker.md)).

Before you start: Stage 7 has run in the target (`databricks bundle run rag -t <target>` has
recorded a `tuned` retriever), and serverless GPU works there (stage 7's runbook, part D, starts
with the check). `make rankcheck` needs OCaml, as `make doccheck` does. Every time is UTC.

## Part A: dev

### A1. Run

```bash
make deploy
databricks bundle run reranker -t dev
```

About 30 to 60 minutes. ✅ `TERMINATED SUCCESS` with three tasks:

- `prepare` prints the manifest, with `answerable` above 0 for every split, then a line per split
  for each candidate source, `dense`, `bm25` and `hybrid`, with hit@1, hit@5, MRR@10 and hit@20,
  and ends `OK: wrote the top 20 candidates of <n> questions`. Note the three hit@20 values of the
  test split: they say whether hybrid candidates would raise the reranker's ceiling.
- `train` (its log is in the `pitlake-reranker-dev` MLflow experiment, under the run's
  `logs/node_0` artifacts) prints `epoch 0 (base) validation mrr_at_10 ...`, a line per epoch,
  `kept epoch <n>`, and ends `OK: trained and reranked <n> test questions twice`.
- `score` prints a line each for `tuned`, `base` and `lora` with hit@1, hit@5, MRR@10 and
  `future 0`, then the answers of `tuned` and `lora` from the chat endpoint, and ends
  `OK: recorded reranker run <run ID> (rerankings <sha>)`.

❌ `prepare` fails with `no tuned retrieval run recorded yet`: run Stage 7's `rag` job first.

### A2. Check it independently

Within seven days of the run (the operations job then clears its folder), with the run ID from
the run URL:

```bash
eval $(opam env)
make rankcheck TARGET=dev RUN=<run ID>
```

✅ The recorded and rebuilt hashes are equal, every metric line has no `<- differs`, both models
report `0 of ... ranked passages knowable after the question, 0 of another company`, and the last
line starts with `IDENTICAL`.

## Part B: prod

```bash
make deploy-prod
databricks bundle run reranker -t prod
```

About 1 to 2 hours. If the CLI loses the connection, follow the run with
`databricks jobs get-run <run ID>`. Then:

```bash
make rankcheck TARGET=prod RUN=<run ID>
```

```sql
SELECT retriever, metrics['tuned_mrr_at_10'] AS tuned_mrr, metrics['mrr_at_10'] AS mrr,
       metrics['tuned_hit_at_1'] AS tuned_hit_1, metrics['hit_at_1'] AS hit_1,
       metrics['hit_at_20'] AS ceiling, metrics['future_chunks'] AS future,
       metrics['answer_correct'] AS answers_correct, metrics['tuned_answer_correct'] AS tuned_answers
FROM pitlake_prod.control.retrieval_runs
WHERE retrieval_run_id = '<run ID>'
ORDER BY retriever;
```

✅ The exit test: on the `rerank_lora` row, `mrr` is above `tuned_mrr` and `hit_1` above
`tuned_hit_1`, `future` is 0, and `make rankcheck` printed IDENTICAL. The numbers go into ADR 0017
under "Result".

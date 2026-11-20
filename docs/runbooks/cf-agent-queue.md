# Counterfactual runbook: the agent's queue

The `agent_queue` job runs every experiment that samples the agent, one after another, so no two
share the Llama 3.3 endpoint ([ADR 0032](../adr/0032-agent-queue.md)): the Stage 3 attribution,
the Stage 8 data value, the Stage 6 monotonicity, the Stage 4 sufficiency and the Stage 5 horizon
of the agent at the instants of one backtest, then the Stage 7 decomposition of one endpoint. Each
is its own run of its own job, with its output under that job, as in its runbook's Part B.

Use it for the agent's half of Part B in the Stage 3 to 8 runbooks once the strategy's half is
done. While it runs, start nothing else that calls `databricks-meta-llama-3-3-70b-instruct`.

## 1. Run a pass

```bash
make deploy-prod
databricks bundle run agent_queue -t prod --params "backtest=a4d4c4e0955046b8a6fb6a171f8b1071,at=2026-06-01 2026-08-27 2026-10-08,decomposition_backtest=175558839963989,cutoff=2024-01-01"
```

Up to about twelve hours: each experiment stops starting worlds after 100 minutes. ✅ `TERMINATED
SUCCESS` and six task runs, each `SUCCESS`. A task that failed does not stop the ones after it;
send Claude its run ID.

## 2. What each experiment printed

```bash
databricks jobs get-run <queue run ID> --profile pitlake -o json | jq -r '.tasks[] | [.task_key, .state.result_state] | @tsv'
```

For each experiment, the last line of its run under its own job (`pitlake-attribution-prod` and
the others) is `OK` or `PARTIAL`. Send Claude the queue's run ID: Claude reads every output and
the tables.

## 3. Repeat until every experiment is OK

While any experiment printed `PARTIAL`, run the same command as in step 1 again. An experiment
already `OK` ends within minutes from its stored worlds, and the others continue where they
stopped. Then follow Part B of each stage's runbook from its independent check on.

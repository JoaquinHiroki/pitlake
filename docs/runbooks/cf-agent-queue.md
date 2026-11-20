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
databricks bundle run agent_queue -t prod --params "agent_backtest=a4d4c4e0955046b8a6fb6a171f8b1071,agent_at=2026-06-01 2026-08-27 2026-10-08,decomposition_backtest=175558839963989,decomposition_cutoff=2024-01-01"
```

Up to about twelve hours: each experiment stops starting worlds after 100 minutes. ✅ `TERMINATED
SUCCESS` and six task runs, each `SUCCESS`. A task that failed does not stop the ones after it;
send Claude its run ID.

## 2. Where each experiment stands

```bash
make status TARGET=prod
```

Under a minute. One entry per stage and decider, the latest experiment of each (`make status
TARGET=prod STATUS=--all` shows every one), newest first within a stage:
its latest run, whether it is `RUNNING` or `QUEUED`, or the `OK`, `PARTIAL` or `FAILED` line it
printed last; then the queue's tasks. Send Claude the output.

## 3. Repeat until every experiment is OK

While any experiment printed `PARTIAL`, run the same command as in step 1 again. An experiment
already `OK` ends within minutes from its stored worlds, and the others continue where they
stopped. Then follow Part B of each stage's runbook from its independent check on.

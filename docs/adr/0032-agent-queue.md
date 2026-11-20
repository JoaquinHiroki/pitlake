# ADR 0032: One queue runs the agent's experiments so none shares the endpoint

Status: proposed (counterfactual information, operations). Builds on
[ADR 0025](0025-attribution.md), [ADR 0026](0026-sufficiency.md), [ADR 0027](0027-horizon.md),
[ADR 0028](0028-monotonicity.md), [ADR 0029](0029-decomposition.md) and
[ADR 0030](0030-data-value.md).

## Context

The agent's half of Stages 3 to 8 runs in six jobs, each of which samples the Llama 3.3 serving
endpoint for up to two hours and stops `PARTIAL` when its `minutes` run out. On 2026-11-20 two of
them ran at once in prod: the decomposition's tooled decisions went from about 4.5 to 13 minutes
each, and the attribution's samples from about 18 to 176 seconds. The endpoint's throughput is
shared, so two jobs together finish later than the same two one after the other. Each job also
allows only one run of itself at a time, so a second run started early waits in the job's queue,
which looked like a hang.

Run by hand, the agent's backlog is a sequence of commands, each of which has to wait for the one
before and may need repeating, over several days.

## Decisions

**A queue job runs the agent's jobs in order.** `agent_queue` has one task per job: attribution,
data value, monotonicity, sufficiency and horizon on the agent at the instants of one backtest,
then the decomposition of one endpoint. Each task is a `run_job_task` that starts the existing job
with its parameters, so each experiment keeps its own environment, timeout, output and record, and
its run is found under its own job as before. The queue adds no code.

**Each task starts when the one before ends, however it ended.** `run_if: ALL_DONE` lets a
failure or a timeout in one experiment leave the others to run. The jobs exit successfully when
they print `PARTIAL`, so a pass of the queue ends with every experiment either `OK` or further
along. Running the queue again continues each from where it stopped, and an experiment already
`OK` finishes at once from its stored worlds.

**The queue's parameters are named apart from every job's.** Databricks passes a job's
parameters down to every job its `run_job_task`s start, beside the parameters each task sets.
With a queue parameter `at` or `backtest`, the decomposition would have received the agent's three
instants and possibly the agent's backtest. So the queue takes `agent_backtest`, `agent_at`,
`decomposition_backtest`, `decomposition_endpoint` and `decomposition_cutoff`, which no job
declares, and each task maps them onto its job's own names. A job ignores the names it does not
declare.

**`make status TARGET=<target>` shows where every experiment stands.** It reads the recent runs
of each counterfactual job, groups them by experiment (the job's own parameters but `minutes`),
and prints, per experiment, the latest run's state: running or queued, or the `OK`, `PARTIAL` or
`FAILED` line it printed last. Then it prints the latest queue run's tasks. It only reads.

**Data value asks both deciders.** Its strategy worlds are stored from Stage 8 B2 and are reused,
so asking both gives the report a line per source for the strategy, the agent and both, as Stage 8
B3 expects.

## Consequences

- A pass takes up to about twelve hours, two per experiment, and needs no attention. Nothing else
  that calls the endpoint should run meanwhile, including a second pass: the queue allows one run
  at a time.
- The queue covers only the experiments that sample the agent. The strategy's, which need no
  endpoint, are run from their runbooks.
- The first queue run, started 2026-11-20 with parameters named `backtest` and `at`, showed the
  push-down in `make status`: its attribution run carried `cutoff` and `endpoint`. It was
  cancelled before its decomposition task started.
- Tests: `tests/test_queue.py` (the order and dependencies, each task's job and parameters being
  ones that job takes, no queue parameter named as any job's, and the deciders asked) and
  `tests/test_status.py` (a run's verdict line, its experiment without `minutes` and parameters
  pushed down, its state, the latest run per experiment, and the printed lines).

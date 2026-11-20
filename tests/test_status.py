from datetime import UTC, datetime

from pitlake.status import (
    QUEUE_ORDER,
    STAGES,
    Run,
    clip,
    current,
    experiment,
    family,
    job_name,
    latest,
    queue_lines,
    queue_rank,
    render,
    state,
    verdict,
)

EARLY = datetime(2026, 11, 20, 10, tzinfo=UTC)
LATE = datetime(2026, 11, 20, 12, tzinfo=UTC)
AGENT = {"backtest": "a4d4c4e0955046b8a6fb6a171f8b1071", "deciders": "agent", "minutes": "100"}


def run(run_id, started, params=AGENT, life="TERMINATED", result="SUCCESS", logs=None):
    return Run(run_id, started, dict(params), life, result, logs)


def test_the_last_verdict_line_is_the_run_s():
    logs = (
        "ATTRIBUTED: agent at ...\nPARTIAL: agent at 2026-10-08\nPARTIAL: 1 of 3 decisions left\n"
    )
    assert verdict(logs) == ("PARTIAL", "1 of 3 decisions left")
    assert verdict("RELEASED: x\nOK: 6 bundles released\n  databricks fs cp a .\n") == (
        "OK",
        "6 bundles released",
    )
    assert verdict("FAILED: no part settles") == ("FAILED", "no part settles")
    assert verdict("nothing here") is None and verdict(None) is None
    assert verdict("OKAY: not a verdict") is None


def test_an_experiment_is_its_parameters_but_minutes_and_those_the_job_lacks():
    assert experiment(AGENT) == "backtest=a4d4c4e0 deciders=agent"
    assert experiment({**AGENT, "minutes": "0"}) == experiment(AGENT)
    pushed = {**AGENT, "cutoff": "2024-01-01", "agent_at": "2026-06-01"}
    known = frozenset({"backtest", "deciders", "minutes"})
    assert experiment(pushed, known) == experiment(AGENT)
    assert experiment(pushed) != experiment(AGENT)
    assert experiment({"at": ""}) == "defaults"
    assert experiment({"decomposition_backtest": "175558839963989"}).endswith("=17555883")


def test_a_run_s_state_comes_from_its_life_cycle_then_its_verdict():
    assert state(run(1, LATE, life="RUNNING", result=None)) == ("RUNNING", "")
    assert state(run(1, LATE, life="QUEUED", result=None)) == ("QUEUED", "")
    assert state(run(1, LATE, result="CANCELED")) == ("CANCELED", "")
    assert state(run(1, LATE, logs="OK: 3 decisions")) == ("OK", "3 decisions")
    assert state(run(1, LATE, logs="no verdict")) == ("SUCCESS", "")


def test_the_latest_run_of_each_experiment_is_shown():
    strategy = {**AGENT, "deciders": "strategy"}
    runs = [
        run(1, EARLY, logs="PARTIAL: 2 left"),
        run(2, LATE, life="RUNNING", result=None),
        run(3, EARLY, strategy, logs="OK: 3 decisions"),
    ]
    lines = latest("3", "attribution", runs)
    assert [(line.run_id, line.state, line.runs) for line in lines] == [
        (2, "RUNNING", 2),
        (3, "OK", 1),
    ]
    text = render(lines)
    assert text[0].startswith(" 3 attribution     RUNNING    run 2 11-20 12:00 (2 runs)")
    assert text[1] == "     backtest=a4d4c4e0 deciders=agent"
    assert text[4] == "     3 decisions" and "(1 run)" in text[2]


def test_long_verdicts_are_clipped_and_the_queue_lists_its_tasks():
    assert clip("x" * 200).endswith("...") and len(clip("x" * 200)) == 110
    assert clip("short") == "short"
    lines = queue_lines(9, LATE, [("attribution", "RUNNING", ""), ("horizon", "BLOCKED", "")])
    assert lines == [
        "queue run 9 started 11-20 12:00",
        "  attribution     RUNNING",
        "  horizon         BLOCKED",
    ]


def test_every_counterfactual_job_is_read():
    assert [job for _, job in STAGES][2:] == [
        "attribution",
        "sufficiency",
        "horizon",
        "monotonicity",
        "decomposition",
        "data-value",
        "release",
    ]
    assert job_name("data-value", "prod") == "pitlake-data-value-prod"


def test_a_running_run_is_shown_before_a_later_queued_one():
    runs = [run(1, EARLY, life="RUNNING", result=None), run(2, LATE, life="QUEUED", result=None)]
    (line,) = latest("3", "attribution", runs)
    assert (line.run_id, line.state, line.runs) == (1, "RUNNING", 2)


def test_the_queue_s_tasks_are_listed_in_its_order():
    shuffled = ["horizon", "attribution", "decomposition", "data_value"]
    assert sorted(shuffled, key=queue_rank) == [
        "attribution",
        "data_value",
        "horizon",
        "decomposition",
    ]
    with open("resources/jobs.yml") as found:
        import yaml

        tasks = yaml.safe_load(found)["resources"]["jobs"]["agent_queue"]["tasks"]
    assert tuple(t["task_key"] for t in tasks) == QUEUE_ORDER


def test_by_default_only_the_latest_experiment_per_stage_and_decider_is_shown():
    strategy = {**AGENT, "deciders": "strategy"}
    runs = [
        run(1, EARLY, {**AGENT, "samples": "5"}, logs="OK: old"),
        run(2, LATE, AGENT, logs="PARTIAL: new"),
        run(3, EARLY, strategy, logs="OK: strategy"),
    ]
    lines = latest("3", "attribution", runs)
    assert len(lines) == 3
    assert sorted((line.run_id, line.family) for line in current(lines)) == [
        (2, "agent"),
        (3, "strategy"),
    ]
    assert family({"endpoint": "maverick", "cutoff": "2024-09-01"}) == "maverick"
    assert family({"backtest": "x"}) == ""

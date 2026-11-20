import re

import yaml

ORDER = ["attribution", "data_value", "monotonicity", "sufficiency", "horizon", "decomposition"]
LLM_JOBS = set(ORDER)


def jobs():
    with open("resources/jobs.yml") as found:
        return yaml.safe_load(found)["resources"]["jobs"]


def test_the_queue_runs_the_agent_jobs_one_after_another():
    queue = jobs()["agent_queue"]
    tasks = queue["tasks"]
    assert [t["task_key"] for t in tasks] == ORDER
    assert queue["max_concurrent_runs"] == 1
    assert "depends_on" not in tasks[0]
    for before, task in zip(tasks, tasks[1:], strict=False):
        assert task["depends_on"] == [{"task_key": before["task_key"]}]
        assert task["run_if"] == "ALL_DONE"


def test_each_task_runs_its_job_with_parameters_the_job_takes():
    found = jobs()
    queue = found["agent_queue"]
    mine = {p["name"] for p in queue["parameters"]}
    for task in queue["tasks"]:
        run = task["run_job_task"]
        (key,) = re.fullmatch(r"\$\{resources\.jobs\.(\w+)\.id\}", run["job_id"]).groups()
        assert key == task["task_key"]
        theirs = {p["name"] for p in found[key]["parameters"]}
        assert set(run["job_parameters"]) <= theirs, key
        for value in run["job_parameters"].values():
            assert set(re.findall(r"\{\{job\.parameters\.(\w+)\}\}", value)) <= mine


def test_the_queue_asks_only_the_agent_and_its_decomposition():
    tasks = {
        t["task_key"]: t["run_job_task"]["job_parameters"] for t in jobs()["agent_queue"]["tasks"]
    }
    for key in ("attribution", "monotonicity", "sufficiency", "horizon"):
        assert tasks[key]["deciders"] == "agent"
    assert tasks["data_value"]["deciders"] == "strategy agent"
    assert tasks["decomposition"]["backtest"] == "{{job.parameters.decomposition_backtest}}"
    assert {t["task_key"] for t in jobs()["agent_queue"]["tasks"]} == LLM_JOBS


def test_no_queue_parameter_shares_a_name_with_a_job_it_starts():
    found = jobs()
    mine = {p["name"] for p in found["agent_queue"]["parameters"]}
    for key in ORDER:
        theirs = {p["name"] for p in found[key]["parameters"]}
        assert not mine & theirs, f"{key} would receive the queue's {sorted(mine & theirs)}"

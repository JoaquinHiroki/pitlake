import re

import yaml

with open(".github/workflows/ci.yml") as found:
    WORKFLOW = yaml.safe_load(found)
DEPLOY = WORKFLOW["jobs"]["deploy"]


def test_deploy_waits_for_every_check_and_skips_pull_requests():
    assert set(DEPLOY["needs"]) == set(WORKFLOW["jobs"]) - {"deploy"}
    assert "github.event_name != 'pull_request'" in DEPLOY["if"]
    assert "refs/heads/main" in DEPLOY["if"]


def test_prod_deploys_only_when_started_by_hand():
    dispatch = WORKFLOW[True]["workflow_dispatch"]["inputs"]["target"]
    assert dispatch["options"] == ["dev", "prod"] and dispatch["default"] == "dev"
    assert DEPLOY["env"]["TARGET"] == "${{ inputs.target || 'dev' }}"


def test_the_cli_is_pinned_and_the_token_is_a_secret():
    setup = next(s["uses"] for s in DEPLOY["steps"] if "setup-cli" in s.get("uses", ""))
    assert re.fullmatch(r"databricks/setup-cli@v\d+\.\d+\.\d+", setup)
    assert DEPLOY["env"]["DATABRICKS_TOKEN"] == "${{ secrets.DATABRICKS_TOKEN }}"
    assert DEPLOY["concurrency"]["cancel-in-progress"] is False

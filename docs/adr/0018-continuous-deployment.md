# ADR 0018: GitHub Actions deploys the bundle after the checks pass

Status: proposed (Stage 9). Builds on [ADR 0001](0001-architecture.md) and
[ADR 0015](0015-runbook-commands-checked.md).

## Context

Spec section 9 says the Asset Bundles are "deployed automatically rather than configured by
hand". Until now CI ran the tests and the runbook check, and every deploy was a person running
`make deploy` or `make deploy-prod` on a laptop. Nothing stopped a deploy of code that had failed
CI, or of a working tree with uncommitted changes, whose `${bundle.git.commit}` then names a
commit that is not what runs.

## Decisions

**A `deploy` job in the CI workflow deploys after `check` and `doccheck` pass.** A push to `main`
deploys `dev`. Prod is deployed only when someone starts the workflow by hand (Actions → CI → Run
workflow, target `prod`); the same checks run first. `workflow_dispatch` works on public and
private repositories alike, where an environment with required reviewers needs a paid plan on a
private one. Pull requests never deploy. One deploy per target runs at a time, and a newer one
waits instead of cancelling a deploy halfway.

**It deploys as the workspace owner, with a personal access token, not as a service principal.**
The bundle names `${workspace.current_user.userName}` 19 times: the bundle's root folder, the
MLflow experiment paths, the failure e-mails, the alerts, and the identity every job runs as. A
deploy by a service principal would resolve all of them to the principal: the bundle would get a
new root and create a second copy of every job, the jobs would run as the principal and need every
Unity Catalog grant, secret scope and the GPU access again, and the experiments would start over
in new folders. A token of the owner makes a CI deploy identical to a laptop deploy, so nothing in
prod moves. The token lives in the repository secret `DATABRICKS_TOKEN`, expires after 90 days,
and the workspace URL is the repository variable `DATABRICKS_HOST`.

**The CLI version is pinned to the laptop's.** CLI v1.17.0 deploys with the direct engine, whose
state in the bundle's root folder is shared by both places that deploy; two CLI versions writing
it could disagree about its format. `databricks/setup-cli@v1.17.0` installs that version, and a
test checks that the pin is an exact version.

## Consequences

- A deploy uses the code of a commit that passed CI. Deploying from the laptop still works and
  shares the same state; the bundle's deployment lock keeps the two from overlapping.
- `bundle deploy` uploads the console's code but does not restart it, as before:
  `databricks bundle run console -t <target>` is still run by hand when the app changed.
- The token must be renewed before it expires; a deploy with an expired token fails at
  `bundle validate`, and GitHub e-mails the failure.
- In a public repository the deploy log shows the workspace user's name, which appears in the
  bundle's paths.
- Moving to a service principal stays possible: set `run_as` and a fixed `workspace.root_path`,
  rebind the existing resources to the new state, and grant the principal what the owner has.
  That is worth it once more than one person deploys.

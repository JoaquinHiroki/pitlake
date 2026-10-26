# ADR 0015: Runbook commands are checked against the repository

Status: proposed (Stage 8). Supports the operations stage's exit test, that another person can
deploy and run the system from the documentation alone ([deploy guide](../runbooks/deploy.md)).

## Context

The runbooks hold about 95 shell commands: `make` targets, `databricks bundle` deploys and runs
with targets and job parameters, and the secrets the collector reads. Each was right when it was
written, and each can go stale when a job parameter is renamed, a target added or a secret moved.
A reader who follows the guide alone has no way to tell a stale command from a real failure.

## Decisions

**`tools/doccheck` reads the facts from the repository and checks every command in the runbooks'
`bash` blocks.** It takes the Makefile's targets, `databricks.yml`'s targets, the jobs and apps in
`resources/*.yml` with each job's parameters, and the secret names the collector code reads. It
reports, by file and line, a `make` target, bundle target, job or app, `--params` name or secret
that does not exist. Placeholders such as `<run ID>` are skipped, as are other code blocks. It
exits non-zero on any finding. `make doccheck` builds it, runs its tests and checks the runbooks,
and CI runs it on every push.

**It is written in OCaml with the standard library only, built with dune.** It is a separate,
self-contained tool, so it adds nothing to the Python packages or the jobs. It depends on no
library beyond the compiler's own standard library: not Jane Street's Core, Base or ppx
rewriters, whose MIT licences would allow it but which the tool does not need. Its tests are plain
assertions. The compiler and standard library are LGPL-2.1 with the static-linking exception, and
dune is MIT, so nothing about the tool's own code or licence changes.

**The configuration is read line by line, not with a YAML parser.** The bundle files follow one
layout, two spaces per level, which the tool relies on: job keys at four spaces under `jobs:` or
`apps:`, job parameters at eight. A change of that layout makes the tool find no jobs, which fails
every `bundle run` check rather than passing silently.

## Consequences

- `make check` stays Python-only, so the deploy guide does not need OCaml. `make doccheck` needs
  opam and dune (`brew install opam`, `opam init`, `opam install dune`).
- On 2026-10-26 all 95 commands in the seven runbooks matched the repository.

export DATABRICKS_CONFIG_PROFILE ?= pitlake

.PHONY: setup lint test check bootstrap validate deploy deploy-prod smoke ingest collector-try collector-dev destroy-dev

setup:
	uv sync --all-packages

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest

check: lint test

bootstrap:
	./scripts/bootstrap.sh

validate:
	databricks bundle validate -t dev

deploy:
	databricks bundle deploy -t dev

deploy-prod:
	databricks bundle deploy -t prod

smoke:
	databricks bundle run smoke -t dev

ingest:
	databricks bundle run ingest -t dev

# Fetch two days into ./data/landing without touching Databricks.
collector-try:
	uv run pitlake-collector sync --config collector/config.example.toml --landing-dir data/landing --work-dir data/work --end 2025-01-02

# Land the dev sample in pitlake_dev from this machine through the Files API (the VM fallback route).
# Normally the ingest job collects it.
collector-dev:
	uv run pitlake-collector sync --config collector/config.dev.toml

# Dev only. There is deliberately no destroy target for prod: it would delete the raw volume.
destroy-dev:
	databricks bundle destroy -t dev

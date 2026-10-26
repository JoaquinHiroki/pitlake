export DATABRICKS_CONFIG_PROFILE ?= pitlake

.PHONY: setup lint test check bootstrap validate deploy deploy-prod smoke ingest transform research rag genie-benchmarks genie-results collector-try collector-dev destroy-dev destroy-rehearsal

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

transform:
	databricks bundle run transform -t dev --params max_files=0

research:
	databricks bundle run research -t dev

rag:
	databricks bundle run rag -t dev

TARGET ?= dev

genie-benchmarks:
	uv run python scripts/genie_benchmarks.py $(TARGET) --start

genie-results:
	uv run python scripts/genie_benchmarks.py $(TARGET)

collector-try:
	uv run pitlake-collector sync --config collector/config.example.toml --landing-dir data/landing --work-dir data/work --end 2025-01-02

collector-dev:
	uv run pitlake-collector sync --config collector/config.dev.toml

# Dev only: there is deliberately no destroy target for prod, it would delete the raw volume.
destroy-dev:
	databricks bundle destroy -t dev

destroy-rehearsal:
	databricks bundle destroy -t rehearsal

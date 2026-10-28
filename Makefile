export DATABRICKS_CONFIG_PROFILE ?= pitlake

.PHONY: setup lint test check bootstrap validate deploy deploy-prod smoke ingest transform research rag genie-benchmarks genie-results tools doccheck pitcheck rankcheck collector-try collector-dev destroy-dev destroy-rehearsal

setup:
	uv sync --all-packages

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest

check: lint test

bootstrap:
	uv run python scripts/bootstrap.py

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

tools:
	cd tools && dune build && dune test

doccheck: tools
	tools/_build/default/doccheck/bin/main.exe .

CATALOG ?= pitlake_$(TARGET)

pitcheck: tools
	uv run python scripts/export_backtest_inputs.py $(CATALOG) $(BACKTEST) data/pitcheck/$(BACKTEST)
	tools/_build/default/pitcheck/bin/main.exe data/pitcheck/$(BACKTEST)

rankcheck: tools
	uv run python scripts/export_rerank_inputs.py $(CATALOG) $(RUN) data/rankcheck/$(RUN)
	tools/_build/default/rankcheck/bin/main.exe data/rankcheck/$(RUN)

collector-try:
	uv run pitlake-collector sync --config collector/config.example.toml --landing-dir data/landing --work-dir data/work --end 2025-01-02

collector-dev:
	uv run pitlake-collector sync --config collector/config.dev.toml

# Dev only: there is deliberately no destroy target for prod, it would delete the raw volume.
destroy-dev:
	databricks bundle destroy -t dev

destroy-rehearsal:
	databricks bundle destroy -t rehearsal

export DATABRICKS_CONFIG_PROFILE ?= pitlake

.PHONY: setup lint test check bootstrap validate deploy deploy-prod smoke destroy-dev

setup:
	uv sync

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

# Dev only. There is deliberately no destroy target for prod: it would delete the raw volume.
destroy-dev:
	databricks bundle destroy -t dev

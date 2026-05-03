IMAGE ?= hive-oracle-etl:0.1.0

.PHONY: test lint build run-local

test:
	python -m pytest -q

lint:
	python -m ruff check src tests

build:
	docker build -t $(IMAGE) .

run-local:
	PYTHONPATH=src python -m hive_oracle_etl.main --env-file .env.example --dry-run --print-query

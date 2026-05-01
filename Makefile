.PHONY: help install lint format test test-unit test-integration up down logs reset seed smoke bronze-once bronze-count silver-once silver-count gold-once gold-count replay-demo airflow-up airflow-dags benchmark-small clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

install: ## Install all deps via uv
	uv sync --all-extras

lint: ## Run ruff + mypy
	uv run ruff check .
	uv run mypy

format: ## Format code with ruff
	uv run ruff format .
	uv run ruff check --fix .

test-unit: ## Run unit tests
	uv run pytest tests/unit/ -v

test-integration: ## Run integration tests
	uv run pytest tests/integration/ -v -m integration

test: test-unit ## Run unit tests

up: ## Bring up local stack
	docker compose up -d

down: ## Stop local stack
	docker compose down

logs: ## Tail stack logs
	docker compose logs -f --tail=100

reset: ## Reset local state
	./scripts/reset_local_state.sh

seed: ## Seed MinIO landing data
	uv run python scripts/seed_local_data.py

smoke: ## Run Spark smoke read
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_smoke.py

bronze-once: ## Process current Binance landing files into Bronze Delta once
	docker compose exec -e BRONZE_TRIGGER_AVAILABLE_NOW=true spark-master /opt/spark/bin/spark-submit /opt/app/jobs/bronze_binance_stream.py

bronze-count: ## Read Bronze Binance Delta and print row count
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/bronze_count.py

silver-once: ## Process Bronze Binance trades into Silver Delta once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/silver_trades.py

silver-count: ## Read Silver trades/quarantine Delta and print row counts
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/silver_count.py

gold-once: ## Build trades-derived Gold Delta tables once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/daily_volume.py
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/market_quality.py

gold-count: ## Read trades-derived Gold Delta tables and print row counts
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/gold_count.py

replay-demo: ## Demo quarantine replay moving one row into Silver
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/replay_demo.py

airflow-up: ## Bring up local Airflow webserver and scheduler
	docker compose --profile airflow up -d --build --force-recreate postgres-airflow airflow-init airflow-webserver airflow-scheduler

airflow-dags: ## List parsed Airflow DAGs
	docker compose --profile airflow run --rm airflow-scheduler airflow dags list

benchmark-small: ## Run local small Delta optimization benchmark
	.venv/bin/python benchmarks/run_optimization_benchmark.py --rows 100000 --iterations 3

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

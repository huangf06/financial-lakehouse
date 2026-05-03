.PHONY: help install lint format test test-unit test-integration validate-release deploy-check deploy-check-strict deploy-local deploy-live e2e-local up down logs reset seed seed-bars smoke bronze-once bronze-bars-once bronze-count silver-once silver-bars-once silver-count gold-once gold-bars-5m-once gold-count replay-demo bars-demo optimize-hot-once optimize-zorder-once vacuum-once airflow-up airflow-dags metrics-snapshot benchmark-small seed-replay replay-bronze-demo bronze-replay-count soak-binance clean

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

validate-release: ## Run release validation before deploying
	.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .
	.venv/bin/mypy
	docker compose config --quiet

deploy-check: ## Check compose deployment prerequisites
	.venv/bin/python scripts/deploy_check.py

deploy-check-strict: ## Check deployment prerequisites and reject local-only secrets
	.venv/bin/python scripts/deploy_check.py --strict

deploy-local: deploy-check ## Start the deployable local stack
	docker compose up -d --build minio minio-init spark-master spark-worker-1 spark-worker-2 metrics-publisher prometheus grafana

deploy-live: ## Start local stack plus live market data producers
	.venv/bin/python scripts/deploy_check.py --strict --live
	docker compose --profile streaming --profile live up -d --build minio minio-init spark-master spark-worker-1 spark-worker-2 spark-bronze producer-binance producer-alpaca metrics-publisher prometheus grafana

e2e-local: ## Run local deployable Bronze -> Silver -> Gold evidence chain
	$(MAKE) seed
	$(MAKE) smoke
	$(MAKE) bronze-once
	$(MAKE) bronze-count
	$(MAKE) silver-once
	$(MAKE) silver-count
	$(MAKE) gold-once
	$(MAKE) gold-count
	$(MAKE) metrics-snapshot

up: ## Bring up local stack
	docker compose up -d

down: ## Stop local stack
	docker compose down

logs: ## Tail stack logs
	docker compose logs -f --tail=100

reset: ## Reset local state
	./scripts/reset_local_state.sh

seed: ## Seed MinIO landing data
	PROFILE=compose S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin LAKEHOUSE_ROOT=s3a://lakehouse CHECKPOINT_ROOT=s3a://lakehouse-meta/_checkpoints uv run python scripts/seed_local_data.py

seed-bars: ## Seed MinIO landing data with synthetic Alpaca bars
	PROFILE=compose S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin LAKEHOUSE_ROOT=s3a://lakehouse CHECKPOINT_ROOT=s3a://lakehouse-meta/_checkpoints uv run python scripts/seed_alpaca_bars.py

smoke: ## Run Spark smoke read
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_smoke.py

bronze-once: ## Process current Binance landing files into Bronze Delta once
	docker compose exec -e BRONZE_TRIGGER_AVAILABLE_NOW=true spark-master /opt/spark/bin/spark-submit /opt/app/jobs/bronze_binance_stream.py

bronze-bars-once: ## Process current Alpaca bar landing files into Bronze Delta once
	docker compose exec -e BRONZE_TRIGGER_AVAILABLE_NOW=true spark-master /opt/spark/bin/spark-submit /opt/app/jobs/bronze_alpaca_bars.py

bronze-count: ## Read Bronze Binance Delta and print row count
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/bronze_count.py

silver-once: ## Process Bronze Binance trades into Silver Delta once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/silver_trades.py

silver-bars-once: ## Process Bronze Alpaca bars into Silver Delta once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/silver_bars.py

silver-count: ## Read Silver trades/quarantine Delta and print row counts
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/silver_count.py

gold-once: ## Build trades-derived Gold Delta tables once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/daily_volume.py
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/market_quality.py

gold-bars-5m-once: ## Build Gold 5m bars table once
	docker compose exec -e GOLD_TIMEFRAME=5m spark-master /opt/spark/bin/spark-submit /opt/app/jobs/gold_bars.py

gold-count: ## Read trades-derived Gold Delta tables and print row counts
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/gold_count.py

replay-demo: ## Demo quarantine replay moving one row into Silver
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/replay_demo.py

bars-demo: ## Demo Alpaca bars Silver split and Gold 5m aggregation
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/bars_demo.py

optimize-hot-once: ## Compact recent Silver trades partitions once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/optimize_hot.py

optimize-zorder-once: ## Run Z-order optimize on Silver trades once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/optimize_zorder.py

vacuum-once: ## Run Delta VACUUM on Silver trades once
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/jobs/vacuum.py

airflow-up: ## Bring up local Airflow webserver and scheduler
	docker compose --profile airflow up -d --build --force-recreate postgres-airflow airflow-init airflow-webserver airflow-scheduler

airflow-dags: ## List parsed Airflow DAGs
	docker compose --profile airflow run --rm airflow-scheduler airflow dags list

metrics-snapshot: ## Print one metrics snapshot from Delta logs
	docker compose run --rm metrics-publisher python -m metrics_publisher.snapshot

benchmark-small: ## Run local small Delta optimization benchmark
	.venv/bin/python benchmarks/run_optimization_benchmark.py --rows 100000 --iterations 3

seed-replay: ## Generate a deterministic replay Parquet (~50 rows) under data/replay/
	.venv/bin/python scripts/seed_replay_parquet.py

replay-bronze-demo: seed-replay ## End-to-end: replay Parquet -> MinIO landing/replay/binance -> Bronze
	S3_ENDPOINT=$${S3_ENDPOINT:-http://localhost:9000} \
	S3_ACCESS_KEY=$${S3_ACCESS_KEY:-minioadmin} \
	S3_SECRET_KEY=$${S3_SECRET_KEY:-minioadmin} \
	.venv/bin/python scripts/replay_from_history.py \
		--source replay/binance \
		--parquet data/replay/binance_2024-01-01.parquet \
		--landing-root s3a://lakehouse/landing \
		--speedup 1000.0
	docker compose exec -e BRONZE_TRIGGER_AVAILABLE_NOW=true spark-master /opt/spark/bin/spark-submit /opt/app/jobs/bronze_replay_binance.py

bronze-replay-count: ## Print Bronze replay-binance Delta row count
	docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/bronze_replay_count.py

soak-binance: ## Run the 4h Binance public WS soak (writes evidence under docs/showcase/soak/_raw/)
	bash scripts/run_binance_soak.sh

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

# Financial Data Lakehouse

Local, resume-defensible financial data lakehouse built around Spark Structured Streaming,
Delta Lake, Airflow, MinIO, Prometheus, and Grafana.

The current local MVP proves the full Bronze -> Silver -> Gold path on Docker Compose:

- synthetic Binance-like JSONL lands in MinIO,
- Spark Structured Streaming writes Bronze Delta with checkpoints,
- Silver normalizes trades and splits quality failures into quarantine,
- Gold builds trades-derived daily volume and market-quality tables,
- Airflow parses the orchestration DAGs against a real local Airflow runtime,
- Prometheus metrics are populated from Delta transaction logs,
- a small Delta optimization benchmark records compact and Z-order timings.

## Quick Start

```bash
uv sync --all-extras
docker compose up -d minio minio-init spark-master spark-worker-1 spark-worker-2
.venv/bin/python scripts/seed_local_data.py
make smoke
make bronze-once
make bronze-count
make silver-once
make silver-count
make gold-once
make gold-count
```

For a continuous Bronze stream instead of a one-shot run:

```bash
docker compose --profile streaming up -d spark-bronze
```

Local service URLs:

- MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`)
- Spark master UI: http://localhost:8080
- Airflow webserver: http://localhost:8081 (`admin` / `admin`)
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (`admin` / `admin`)

## Validation

```bash
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy
docker compose config --quiet
```

Current recorded validation is in
`docs/progress/2026-05-01-codex-execution-notes.md`.

## Runtime Evidence

| Evidence | Command | Expected local result |
|---|---|---|
| MinIO landing smoke read | `make smoke` | Spark reads seeded JSONL from `s3a://lakehouse/landing/binance` |
| Bronze checkpointed stream | `make bronze-once && make bronze-count` | Bronze Binance Delta table contains 100 records after two seed runs |
| Silver quality loop | `make silver-once && make silver-count` | Silver trades has 100 records; trade quarantine has 0 for clean synthetic data |
| Quarantine replay | `make replay-demo` | One quarantined row moves into Silver after replay rule adjustment |
| Gold trades-derived tables | `make gold-once && make gold-count` | Daily volume and market quality each contain 3 symbol-level rows |
| Bars Silver/Gold loop | `make seed-bars && make bronze-bars-once && make silver-bars-once && make gold-bars-5m-once` | Five valid Alpaca bars form one Gold 5m bar; one invalid bar is quarantined |
| Delta maintenance jobs | `make optimize-hot-once && make optimize-zorder-once && make vacuum-once` | Local Spark runs OPTIMIZE and VACUUM against Delta path tables in MinIO |
| Airflow runtime | `make airflow-up && make airflow-dags` | DAG list includes Silver, Gold, replay, optimize, and vacuum DAGs |
| Metrics publisher | `docker compose build metrics-publisher && make metrics-snapshot` | Prints real Bronze, Silver, and Gold Delta table counts from MinIO logs |
| Optimization benchmark | `make benchmark-small` | Updates `benchmarks/results.md` with baseline, compact, and Z-order timings |

## Architecture

Producers write atomically rotated JSONL files into landing storage. Bronze reads those files
with Structured Streaming, writes Delta tables, preserves raw source metadata, and persists
checkpoints. Silver normalizes market events and applies a declarative quality framework that
splits valid records from quarantine. Replay jobs can re-evaluate quarantined rows after rule
changes. Gold builds analytical aggregates for daily volume and market quality. Airflow DAGs
orchestrate Silver, Gold, quarantine replay, optimization, and vacuum maintenance.

Local compose uses MinIO as S3-compatible storage and Spark standalone for execution. The
Databricks-specific Bronze reader branch is present for Auto Loader, but the local evidence path
uses OSS Spark file streaming with a fixed schema; additive producer fields are tolerated and
dropped locally, while Auto Loader is the intended true schema-evolution runtime.

## Resume Claims To Evidence

| Claim | Evidence |
|---|---|
| Structured Streaming with schema evolution and checkpoints | `pipelines/bronze/stream_reader.py`, `pipelines/bronze/writer.py`, `tests/integration/test_schema_evolution.py`, `tests/integration/test_checkpoint_recovery.py`, `make bronze-once` |
| Quality framework with quarantine-and-replay | `pipelines/quality/framework.py`, `pipelines/quality/replay.py`, `pipelines/quality/rules/`, `tests/integration/test_e2e_pipeline.py`, `make replay-demo` |
| Delta partitioning, compaction, and Z-order | `pipelines/schemas/partition_spec.py`, `pipelines/maintenance/optimize.py`, `tests/integration/test_z_order_benefit.py`, `benchmarks/results.md` |
| Airflow orchestration and failure alerting | `dags/`, `dags/_common/callbacks.py`, `docker-compose.yml`, `make airflow-dags` |
| Observability over lakehouse health | `metrics_publisher/publisher.py`, `metrics_publisher/queries.py`, `observability/prometheus/prometheus.yml`, `observability/grafana/dashboards/lakehouse-overview.json` |

## Important Commands

```bash
make seed             # seed 50 synthetic Binance trades into MinIO
make seed-bars        # seed synthetic Alpaca 1m bars into MinIO
make bronze-once      # process available landing files into Bronze
make bronze-bars-once # process Alpaca bar landing files into Bronze
make silver-once      # process Bronze Binance trades into Silver
make silver-bars-once # process Bronze Alpaca bars into Silver
make gold-once        # build trades-derived Gold tables
make gold-bars-5m-once # build Gold 5m bars table
make replay-demo      # local quarantine replay demonstration
make bars-demo        # local Silver bars + Gold 5m demonstration
make optimize-hot-once # compact recent Silver trades partitions
make optimize-zorder-once # run Z-order optimize on Silver trades
make vacuum-once      # run Delta VACUUM on Silver trades
make airflow-up       # start local Airflow profile
make airflow-dags     # list parsed Airflow DAGs
make metrics-snapshot # print one Prometheus metric snapshot from Delta logs
make benchmark-small  # run 100k-row local Delta optimization benchmark
```

## Project Documents

- Design: `docs/superpowers/specs/2026-04-30-financial-lakehouse-design.md`
- MVP plan: `docs/superpowers/plans/2026-04-30-financial-lakehouse-mvp.md`
- Engineering review: `docs/progress/2026-05-01-engineering-review.md`
- Execution notes: `docs/progress/2026-05-01-codex-execution-notes.md`
- Current status: `docs/progress/2026-05-02-project-status.md`
- Benchmark results: `benchmarks/results.md`

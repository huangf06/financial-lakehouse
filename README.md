# Financial Data Lakehouse

Local, resume-defensible financial data lakehouse built around Spark Structured Streaming, Delta Lake, Airflow, MinIO, Prometheus, and Grafana.

## Quick Start

```bash
uv sync --all-extras
make test-unit
docker compose build spark-master
docker compose up -d minio minio-init spark-master spark-worker-1 spark-worker-2
.venv/bin/python scripts/seed_local_data.py
make smoke
make bronze-once
make bronze-count
```

For a continuous Bronze stream instead of a one-shot run:

```bash
docker compose --profile streaming up -d spark-bronze
```

Local service URLs:

- MinIO: http://localhost:9001 (`minioadmin` / `minioadmin`)
- Spark master UI: http://localhost:8080

## Architecture

Producers write atomically rotated JSONL files into `landing/`. Bronze reads those files with Structured Streaming, writes Delta with checkpoints, and preserves source metadata. Silver normalizes data and applies a declarative quality framework that splits valid records from quarantine. Gold builds analytical bars, daily volume, and market-quality outputs. Airflow DAGs orchestrate Silver, Gold, quarantine replay, optimization, and vacuum maintenance.

## Resume Claims To Evidence

| Claim | Where to verify |
|---|---|
| Structured Streaming with schema evolution and checkpoints | `pipelines/bronze/stream_reader.py`, `pipelines/bronze/writer.py`, `tests/integration/test_schema_evolution.py`, `tests/integration/test_checkpoint_recovery.py` |
| Quality framework with quarantine-and-replay | `pipelines/quality/framework.py`, `pipelines/quality/replay.py`, `pipelines/quality/rules/`, `tests/unit/quality/` |
| Delta partitioning, compaction, and Z-order | `pipelines/schemas/partition_spec.py`, `pipelines/maintenance/optimize.py`, `benchmarks/run_optimization_benchmark.py` |
| Airflow orchestration and failure alerting | `dags/`, `dags/_common/callbacks.py`, `docker-compose.yml` |

The detailed design lives in `docs/superpowers/specs/2026-04-30-financial-lakehouse-design.md`.

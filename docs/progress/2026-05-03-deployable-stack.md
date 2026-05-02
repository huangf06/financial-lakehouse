# Deployable Local Stack Progress

Date: 2026-05-03

## Completed

- Connected live producers to the same MinIO landing prefix used by Bronze.
  - `producer-binance` now writes JSONL objects to `s3a://lakehouse/landing/binance`.
  - `producer-alpaca` is available behind the explicit `live` profile and writes to
    `s3a://lakehouse/landing/alpaca`.
- Added an S3-compatible JSONL writer for producers. It buffers rotated records and publishes
  complete objects to S3/MinIO without exposing local `.tmp.*` files.
- Added deployable local stack commands:
  - `make deploy-check`
  - `make deploy-check-strict`
  - `make deploy-local`
  - `make deploy-live`
  - `make e2e-local`
- Added `.env.example` and `scripts/deploy_check.py` for deployment readiness checks.
- Updated MinIO, Grafana, and Airflow local credentials to support `.env` overrides.
- Added local deployment documentation in `docs/deploy/local-compose.md`.

## Validation

Static and test validation:

```text
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
47 passed in 63.05s

.venv/bin/ruff check .
All checks passed!

.venv/bin/ruff format --check .
122 files already formatted

.venv/bin/mypy
Success: no issues found in 64 source files

docker compose config --quiet
passed
```

Deployment validation:

```text
make deploy-local
passed

make e2e-local
passed
```

Observed deployed-stack evidence:

```text
=== BRONZE BINANCE COUNT: 150 records ===
=== SILVER TRADES COUNT: 250 records ===
=== SILVER QUARANTINE COUNT: 0 records ===
=== SILVER BARS COUNT: 5 records ===
=== SILVER BARS QUARANTINE COUNT: 1 records ===
=== GOLD DAILY VOLUME COUNT: 6 records ===
=== GOLD MARKET QUALITY COUNT: 6 records ===
=== GOLD BARS 5M COUNT: 1 records ===

lakehouse_bronze_binance_records_total=150
lakehouse_bronze_alpaca_bars_records_total=6
lakehouse_silver_trades_records_total=250
lakehouse_silver_quarantine_records_total=0
lakehouse_silver_bars_records_total=5
lakehouse_silver_bar_quarantine_records_total=1
lakehouse_gold_daily_volume_records_total=6
lakehouse_gold_market_quality_records_total=6
lakehouse_gold_bars_5m_records_total=1
```

The counts include retained MinIO state from previous local runs. For a clean release rehearsal,
run `make reset`, `make deploy-local`, and `make e2e-local`.

## Current Deployment Boundary

The repository is now deployable as a local Docker Compose lakehouse stack. Before exposing it beyond
localhost or running it as a durable service, replace local credentials in `.env` and run:

```bash
make deploy-check-strict
```

Cloud production deployment remains separate work: AWS/Databricks/Oracle profiles, managed secrets,
remote object storage, TLS, backups, IAM, and external alert routing are not yet implemented.

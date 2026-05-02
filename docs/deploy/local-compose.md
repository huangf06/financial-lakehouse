# Local Compose Deployment

This is the deployable profile for the current repository. It is suitable for a local server,
portfolio demo, or reviewer environment. It is not a hardened internet-facing production deployment.

## Prerequisites

- Docker Compose v2
- Python 3.11 virtual environment with project dependencies installed
- Optional live profile: outbound network access plus Alpaca API credentials

## Configure

```bash
cp .env.example .env
```

For a private local demo, the defaults work. For any shared machine or exposed host, replace at
least:

- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `GRAFANA_ADMIN_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW__WEBSERVER__SECRET_KEY`

The live Alpaca producer also requires:

- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`

## Validate

```bash
make validate-release
make deploy-check
```

Use strict mode before exposing services outside localhost:

```bash
make deploy-check-strict
```

## Deploy Local Stack

```bash
make deploy-local
```

This starts MinIO, Spark, metrics publisher, Prometheus, and Grafana.

Then seed and run the full batch evidence path:

```bash
make e2e-local
```

## Deploy Live Producers

```bash
make deploy-live
```

This starts the local stack, the continuous Bronze stream, Binance trade producer, and Alpaca bar
producer. Producers publish complete JSONL objects directly into MinIO under
`s3a://lakehouse/landing/{source}`, which is the same landing prefix Bronze reads.

## Service URLs

- MinIO: http://localhost:9001
- Spark master: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

Airflow remains an explicit profile:

```bash
make airflow-up
make airflow-dags
```

## Stop

```bash
make down
```

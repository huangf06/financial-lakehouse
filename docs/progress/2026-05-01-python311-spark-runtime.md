# Python 3.11 Spark Runtime Progress

Date: 2026-05-01

## Completed

- Updated `docker/spark/Dockerfile` so the Spark image runs Python 3.11 instead of Ubuntu 20.04's default Python 3.8.
- Added Python 3.11 from `python:3.11-slim-bullseye` via a multi-stage copy into the `apache/spark:3.5.3` image.
- Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `/usr/local/bin/python3.11`.
- Added Spark's bundled PySpark and Py4J zip files to `PYTHONPATH`, so the image uses Spark's PySpark 3.5.3 instead of a pip-installed PySpark.
- Installed `delta-spark==3.2.0` with `--no-deps` to avoid pulling a newer PySpark wheel.
- Removed Python 3.8 compatibility workarounds in settings, quality framework, and replay demo code.

## Runtime Confirmation

```text
docker compose exec spark-master python3 --version
Python 3.11.13

docker compose exec spark-master /usr/local/bin/python3.11 -m pip show pyspark
WARNING: Package(s) not found: pyspark

docker compose exec spark-master python3 - <<'PY'
import pyspark
print(pyspark.__version__)
PY
3.5.3
```

## Validation

```text
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
29 passed in 57.46s

.venv/bin/ruff check .
All checks passed!

.venv/bin/ruff format --check .
108 files already formatted

.venv/bin/mypy
Success: no issues found in 62 source files

docker compose config --quiet
passed

make bronze-once
passed

make bronze-count
=== BRONZE COUNT: 100 records ===

make replay-demo
=== REPLAY DEMO ===
before: silver=0, quarantine=1
after: silver=1, quarantine=0
replayed_trade_ids=['binance:9001']
```

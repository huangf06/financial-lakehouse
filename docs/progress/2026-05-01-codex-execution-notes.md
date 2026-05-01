# Codex Execution Notes

Date: 2026-05-01

## Completed Scope

- Protected the existing Phase 1 work before edits with semantic commits:
  - `7bbae68 feat: add project foundation and schemas`
  - `5150b66 feat: add local infrastructure and orchestration scaffolding`
  - `095d0cd feat: add bronze lakehouse pipelines and tests`
  - `ec72b56 chore: add package and showcase placeholders`
- Wired Spark job storage configuration through `pipelines.config.Settings`:
  - `jobs/_common.py` now accepts an optional `Settings` object and uses validated storage config for S3A.
  - Bronze, Silver, Gold, and quarantine job paths now use `settings.landing_path`, `settings.schema_path`, `settings.checkpoint`, and `settings.table_path`.
  - `scripts/bronze_count.py` now reuses `jobs._common.spark_session()` and handles empty tables without an assertion.
- Rewrote Bronze integration evidence:
  - `tests/integration/test_checkpoint_recovery.py` now exercises `bronze_stream_reader` and `write_bronze_stream` against a tmp local Delta table and checkpoint.
  - `tests/integration/test_schema_evolution.py` now documents and tests the OSS contract: additive unknown fields are tolerated and dropped while known fields continue ingesting.
- Applied bundled P2 cleanups:
  - Removed duplicate Bronze count Spark setup.
  - Fixed `_raw_json` alias symmetry in Silver trades.
  - Removed the unowned `cache()` from the quality split path.
  - Removed redundant `pyspark==3.5.3` installation from the Spark Docker image.
- Added local runtime evidence beyond the original P0/P2 scope:
  - `make replay-demo` demonstrates one quarantined Bronze row moving into Silver after a replay rule change.
  - `make silver-once` runs the compose-level Bronze-to-Silver job against the standard Delta paths.
  - `make silver-count` reports standard Silver trades and quarantine table counts.
  - The Spark image now runs Python 3.11 while preserving Spark 3.5.3 and Delta 3.2.0.
  - The Airflow compose profile starts Postgres, webserver, scheduler, and Spark-backed DAG parsing.
  - The Airflow image copies the Spark 3.5.3 runtime and installs the Spark provider without dependencies, so it does not pull a PySpark 4.x wheel.
- Replaced remaining P1 placeholder-style integration tests with behavior checks:
  - Bronze streaming now writes a real annotated Delta table via `bronze_stream_reader` and `write_bronze_stream`.
  - Airflow Dataset evidence now checks Silver DAG outlets are the Gold DAG inputs.
  - Z-order evidence now checks the maintenance helper emits the partition-spec Z-order statement through `run_optimize`.

## Deviations

- Unit Spark fixtures were updated to initialize a Delta-capable SparkSession too, because the full pytest command runs unit tests before integration tests and Spark reuses the first session created.
- Airflow compose was implemented only after the initial P0/P2 execution completed and subsequent user instructions asked to continue. The provider install is intentionally `--no-deps` to keep the runtime on the pinned PySpark 3.5.3 API exposed by the copied Spark distribution.
- During validation, `make bronze-once` initially waited for resources after Airflow recreated the Spark master while workers were still attached to the old master. I stopped that run, recreated both workers, and reran `make bronze-once` successfully.

## Final Validation

```text
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
29 passed in 60.75s

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

make silver-once
passed

make silver-count
=== SILVER TRADES COUNT: 100 records ===
=== SILVER QUARANTINE COUNT: 0 records ===

make replay-demo
before: silver=0, quarantine=1
after: silver=1, quarantine=0
replayed_trade_ids=['binance:9001']

make airflow-up
started postgres-airflow, airflow-init, airflow-webserver, airflow-scheduler

make airflow-dags
parsed DAGs: gold_aggregations, optimize_hot, optimize_zorder_nightly, quarantine_replay, silver_pipeline, vacuum_nightly

Airflow runtime check
Python 3.11.10; pip show pyspark: not found; imported pyspark.__version__ == 3.5.3
```

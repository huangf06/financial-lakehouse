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
  - `make gold-once` builds the trades-derived Gold Delta tables for daily volume and market quality.
  - `make gold-count` reports Gold table counts and sample rows.
  - The metrics publisher now reads Delta transaction logs in MinIO and publishes real Bronze/Silver/Gold table record gauges.
  - The Spark image now runs Python 3.11 while preserving Spark 3.5.3 and Delta 3.2.0.
  - The Airflow compose profile starts Postgres, webserver, scheduler, and Spark-backed DAG parsing.
  - The Airflow image copies the Spark 3.5.3 runtime and installs the Spark provider without dependencies, so it does not pull a PySpark 4.x wheel.
- Replaced remaining P1 placeholder-style integration tests with behavior checks:
  - Bronze streaming now writes a real annotated Delta table via `bronze_stream_reader` and `write_bronze_stream`.
  - Airflow Dataset evidence now checks Silver DAG outlets are the Gold DAG inputs.
  - Z-order evidence now checks the maintenance helper emits the partition-spec Z-order statement through `run_optimize`.
- Added a reproducible local Delta optimization benchmark:
  - `make benchmark-small` generates a 100k-row synthetic Silver trades table.
  - The harness measures the four benchmark queries across baseline, compact, and `ZORDER BY (symbol)` states.
  - `benchmarks/results.md` records the current median timings.
- Updated handoff-facing docs:
  - `README.md` now contains a concrete quick start, runtime evidence table, validation commands, and claim-to-evidence map.
  - `CLAUDE.md` status now reflects the implemented local MVP instead of the initial no-code handoff.
  - `make metrics-snapshot` prints a one-shot metrics snapshot from the metrics publisher container.
- Hardened Airflow DAG evidence:
  - Maintenance DAGs now use the shared failure callback and retry policy.
  - DAG tests now verify expected task sets and failure callbacks, not only module imports.
- Aligned remaining local utility scripts with typed settings:
  - `scripts/spark_smoke.py` now uses `pipelines.config.load_settings()` and the shared `jobs._common.spark_session()` helper instead of raw `os.environ` Spark configuration.
  - `scripts/seed_local_data.py` now derives S3 credentials, endpoint, bucket, and landing prefix from `Settings`; the `make seed` target supplies the compose profile values for host-side MinIO seeding.
  - Added focused unit coverage for S3/S3A landing path parsing.

## Deviations

- Unit Spark fixtures were updated to initialize a Delta-capable SparkSession too, because the full pytest command runs unit tests before integration tests and Spark reuses the first session created.
- Airflow compose was implemented only after the initial P0/P2 execution completed and subsequent user instructions asked to continue. The provider install is intentionally `--no-deps` to keep the runtime on the pinned PySpark 3.5.3 API exposed by the copied Spark distribution.
- During validation, `make bronze-once` initially waited for resources after Airflow recreated the Spark master while workers were still attached to the old master. I stopped that run, recreated both workers, and reran `make bronze-once` successfully.

## Final Validation

```text
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
32 passed in 67.58s

.venv/bin/ruff check .
All checks passed!

.venv/bin/ruff format --check .
112 files already formatted

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

make gold-once
passed

make gold-count
=== GOLD DAILY VOLUME COUNT: 3 records ===
=== GOLD MARKET QUALITY COUNT: 3 records ===

docker compose build metrics-publisher
passed

make metrics-snapshot
lakehouse_bronze_binance_records_total=100
lakehouse_silver_trades_records_total=100
lakehouse_silver_quarantine_records_total=0
lakehouse_gold_daily_volume_records_total=3
lakehouse_gold_market_quality_records_total=3

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

make smoke
=== SMOKE TEST: read 100 records ===

.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
34 passed in 66.02s

.venv/bin/ruff check .
All checks passed!

.venv/bin/ruff format --check .
113 files already formatted

.venv/bin/mypy
Success: no issues found in 63 source files

docker compose config --quiet
passed

make benchmark-small
rows=100000 iterations=3
q1_single_symbol_24h.sql: baseline=791.57 ms compact=508.73 ms zorder=477.5 ms
q2_single_symbol_7d_vwap.sql: baseline=753.28 ms compact=490.54 ms zorder=518.47 ms
q3_cross_section_1h.sql: baseline=835.0 ms compact=547.57 ms zorder=509.75 ms
q4_count_by_symbol.sql: baseline=577.84 ms compact=454.77 ms zorder=403.33 ms
```

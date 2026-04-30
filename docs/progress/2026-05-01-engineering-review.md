# Financial Lakehouse Engineering Review

Date: 2026-05-01
Reviewer: Claude (Opus 4.7, code-review pass)
Subject report: `docs/progress/2026-05-01-engineering-progress-report.md`
Repo state at review: branch `main`, all implementation files **untracked** (only `e3f9d98` / `6a53a39` / `d262373` docs commits exist).

## TL;DR

Static validation chain is honest (29 passed, ruff/mypy clean, compose validates).
Local `make bronze-once` + second-seed evidence is real and reproducible.
However, the report **overstates evidence in three places** and the entire working tree is **uncommitted**. Fix the items below before treating Phase 1 as "done".

## Verified facts

| Claim | Verified |
|---|---|
| `pytest tests/unit tests/dag tests/integration -q` → 29 passed | Yes (28.31s) |
| `ruff check .` → All checks passed | Yes |
| `ruff format --check .` → 107 files already formatted | Yes |
| `mypy` → no issues in 62 source files | Yes |
| `docker compose config --quiet` → exit 0 | Yes |
| File inventory in §1–§12 of the progress report | Matches |
| 7 ADRs under `docs/decisions/` | Confirmed (0001…0007) |
| 5 GHA workflows under `.github/workflows/` | Confirmed (lint-test, integration-test, dag-validate, build-images, benchmark-small) — not mentioned by report |

## Findings by severity

### P0 — must fix before claiming Phase 1 done

#### P0-1. Entire implementation is untracked

`git status` shows ~100+ untracked files; only design spec, Plan 1, and `CLAUDE.md` are committed. A `git clean -fd` or filesystem incident wipes everything.

**Fix**: stage and commit Phase 1 work in 2–3 semantic commits, e.g.
- `feat: add repo foundation, config, and schemas (Plan 1 Tasks 1–6)`
- `feat: add docker compose stack (MinIO + Spark standalone) (Plan 1 Tasks 10–12)`
- `feat: add Bronze Structured Streaming + checkpoint recovery evidence (Plan 1 Tasks 13–15)`
- `feat: add quality framework + Silver/Gold/maintenance scaffolding (Plan 1 Tasks 16–35)`

**Acceptance**: `git status --porcelain` shows no `??` lines. Each commit message ends with the standard `Co-Authored-By` footer if generated.

#### P0-2. Bronze integration tests bypass production code path

Affects resume bullet 1 ("configured schema evolution and checkpoint-based fault tolerance, validated zero data loss with integration tests").

- `tests/integration/test_checkpoint_recovery.py:33` writes `format("parquet")` on local FS.
  Production (`pipelines/bronze/writer.py:41`) writes Delta on S3A.
- `tests/integration/test_schema_evolution.py:24` enables `spark.sql.streaming.schemaInference=true` with no schema.
  Production (`pipelines/bronze/stream_reader.py:36`) calls `.schema(BINANCE_TRADE_SCHEMA)`, which silently drops unknown JSON fields. The OSS Spark branch as written **cannot** evolve schema; only the Databricks Auto Loader branch can.

**Fix**:
1. Rewrite `test_checkpoint_recovery.py` to call `bronze_stream_reader` + `write_bronze_stream` against a tmp local Delta path (use `delta-spark` with `spark.jars.packages` or rely on already-baked Delta jars; `tmp_path` fixture is fine — no MinIO needed).
2. For `test_schema_evolution.py`, decide the contract:
   - **Option A (recommended)**: Acknowledge OSS limitation. Test that Bronze is `additive-tolerant` — old fields keep, unknown new fields are dropped without breaking the stream. Update test name + docstring.
   - **Option B**: Make the OSS branch use `mergeSchema=true` on read with no fixed schema (schema-on-read). Then test true v1→v2 evolution. This changes production behavior — flag in CLAUDE.md and an ADR.

**Acceptance**: both tests import from `pipelines.bronze` and exercise the real reader/writer. Run `pytest tests/integration/test_checkpoint_recovery.py tests/integration/test_schema_evolution.py -v` — green.

#### P0-3. `Settings` class is dead code; CLAUDE.md is violated

CLAUDE.md states: *"All runtime config via env vars validated by `pydantic-settings` (`pipelines/config/settings.py`). **Never hardcode** paths, endpoints, or credentials."*

But:
- `jobs/_common.py:11–24` uses raw `os.environ.get(..., "minioadmin")`
- `jobs/bronze_binance_stream.py:14–35` uses raw `os.environ.get` for every path
- `scripts/bronze_count.py:18–22` re-implements Spark config inline
- Only `tests/unit/test_config.py` imports `Settings`

**Fix** (recommended path):
1. In `jobs/_common.spark_session()`, accept an optional `Settings` object; default to `load_settings()`.
2. Read `storage.endpoint`, `storage.access_key`, `storage.secret_key`, `storage.path_style_access` from `Settings` instead of `os.environ`.
3. In `jobs/bronze_binance_stream.py`, replace path env-var reads with `settings.landing_path("binance")` / `settings.checkpoint("bronze_binance_trades")` / `settings.table_path("bronze", "binance_trades")`.
4. `scripts/bronze_count.py` should `from jobs._common import spark_session` and reuse it (also fixes finding P2-9).

**Acceptance**: `grep -rn 'os.environ.get' jobs/ scripts/` shows only `LOG_LEVEL`-style operational toggles, not config. Bronze pipeline still works under `make bronze-once`.

### P1 — fix in the next phase

#### P1-4. Four of seven integration tests are pure import-existence asserts

- `tests/integration/test_bronze_streaming.py:9` — `assert bronze_stream_reader is not None`
- `tests/integration/test_e2e_pipeline.py:9` — `assert pipelines.bronze and pipelines.silver and pipelines.gold`
- `tests/integration/test_z_order_benefit.py:9` — string check on SQL text
- `tests/integration/test_dataset_triggers.py:9` — symbol existence check

The progress report's "29 passed" headline conceals this. After Silver compose-level loop is added, replace these with real behavioral tests. Until then, the report should explicitly call them out as `placeholder` (current §10 only mentions one in passing).

#### P1-5. Resume bullet 2 has no end-to-end quarantine-replay evidence

`pipelines/quality/replay.py:20` is exercised on in-memory DataFrames in `test_quarantine_replay.py`, but no job actually reads `silver/quarantine_trades` Delta, runs replay, and writes back. `dags/quarantine_replay.py` and `jobs/quarantine_replay.py` are static modules.

**Fix**: add `make replay-demo` target that seeds 1 invalid record → runs Silver → modifies the rule → runs replay → counts records moved from quarantine to silver. Goes alongside the Silver compose-level loop.

#### P1-6. Resume bullet 4 (Airflow integration) has zero runtime evidence

Plan 1 Task 27 is incomplete: `docker-compose.yml` has no `airflow-webserver` / `airflow-scheduler` / `postgres-airflow` services. DAG modules import only because `from airflow.decorators import dag` is wrapped in `try/except`.

**Fix decision**:
- **Option A**: complete Task 27 (compose Airflow), demo `silver_pipeline` DAG triggering `gold_aggregations` via Datasets. This is honest evidence.
- **Option B**: temporarily soften the resume bullet to "designed Airflow DAG patterns (Datasets, callbacks, SparkSubmitOperator) for dependency-aware orchestration; production runtime via Databricks Jobs." Less impressive but defensible.

Don't ship the bullet as-is without one of these.

#### P1-7. Resume bullet 3 (Z-order) has no benchmark data

`benchmarks/run_optimization_benchmark.py:18–25` writes `{"median_ms": None}`. No real timings exist.

**Fix**: after Silver/Gold are real, run a small benchmark (~100k–500k trade rows, before/after `OPTIMIZE ... ZORDER BY (symbol)`) on 4 query patterns. Generate `benchmarks/results.md` with a table. This is the **easiest** of the four bullets to back with hard evidence.

### P2 — code-quality nits

| # | File:line | Issue | Fix |
|---|---|---|---|
| P2-8 | `scripts/bronze_count.py:32` | `assert count > 0` trips on first run / empty table | Replace with `if count == 0: print("no data yet"); return` |
| P2-9 | `scripts/bronze_count.py:11–24` | Duplicates `jobs/_common.spark_session()` config inline | `from jobs._common import spark_session` |
| P2-10 | `pipelines/silver/trades_pipeline.py:30` | `col("_raw_json") if "_raw_json" in bronze.columns else lit(None).alias("_raw_json")` — `.alias()` only on the lit branch | Apply `.alias("_raw_json")` to both branches for symmetry |
| P2-11 | `pipelines/quality/framework.py:50` | `evaluated.cache()` with no `unpersist()` | `try/finally` or document that caller owns lifecycle |
| P2-12 | `docker/spark/Dockerfile:7` | Re-installs `pyspark==3.5.3` on top of `apache/spark:3.5.3` (already includes PySpark) | Drop `pyspark` from the pip install list; keep `delta-spark`, `pydantic-settings`, `boto3` |

## Cross-check vs Plan 1 (45 tasks)

- Verified-and-running: Tasks 1, 2, 4, 5, 6, 10, 11, 12, 13, 14, 16, 17, 18, 19, 28, 36, 38, 43 — **~18/45**.
- Code present but unverified end-to-end: Tasks 3 (partial), 7, 8, 9, 15 (tests don't match production), 20–26, 29, 30, 31 (partial), 32, 33, 34, 35 (placeholder), 37 (placeholder), 39, 40, 41, 42, 44, 45 — **~25/45**.
- Not started: Task 27 (Airflow services) — **1/45**.

The progress report's "40–45% local MVP" / "25–30% full design" estimates are honest if "code-present" counts as partial credit, but the gap between "code merged" and "behavior validated" is wider than the prose suggests.

## Recommended execution order

1. **P0-1** — commit Phase 1 work (30 min)
2. **P0-3** — wire `jobs/*.py` and `scripts/bronze_count.py` to `Settings` (1 h)
3. **P0-2** — rewrite the two Bronze integration tests against the real reader/writer (1–2 h)
4. **P2-8 / P2-9 / P2-10 / P2-11 / P2-12** — bundled cleanup (30 min)
5. Re-run validation chain; commit P0/P2 fixes (15 min)
6. Move on to Silver compose-level loop (which addresses P1-4 and P1-5 in one phase)
7. Benchmark + Airflow decisions afterward (P1-6, P1-7)

## Out-of-scope for this review

- AWS / Databricks / Oracle deployments (Plans 2–4, deferred per CLAUDE.md)
- Producer live-WebSocket validation (acknowledged in progress report §5)
- README polish / Loom / screenshots (Task 45)

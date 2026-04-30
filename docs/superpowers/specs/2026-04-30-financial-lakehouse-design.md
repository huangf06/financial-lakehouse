# Financial Data Lakehouse — Design Document

**Status:** Approved (2026-04-30)
**Owner:** Fei Huang
**Project type:** Personal portfolio (resume-defensible) + personal trading data tool
**Estimated effort:** 8–10 weekends (~2.5 months calendar)

---

## 1. Context & Goals

This project backfills the four resume bullets under **"Financial Data Lakehouse (Databricks/Spark/AWS)" — Personal Portfolio Project (Oct 2025 – Jan 2026)**:

1. *Architected end-to-end data lakehouse on Databricks processing real-time financial market feeds via Auto Loader and Structured Streaming; configured schema evolution and checkpoint-based fault tolerance, validated zero data loss with integration tests.*
2. *Engineered data quality framework with quarantine-and-replay pattern isolating malformed records across Bronze/Silver/Gold Medallion Architecture layers; achieved automated recovery without manual intervention.*
3. *Optimized Delta Lake performance through Z-ordering on high-cardinality columns and automated compaction resolving small-file fragmentation from streaming ingestion; designed partition strategy aligned with time-series access patterns to minimize scan volume for downstream analytical queries.*
4. *Integrated Airflow (OSS deployment) and Databricks Jobs (managed deployment) for dependency-aware pipeline orchestration with failure alerting; containerized all services via Docker for reproducible deployment across development and production environments.*

(Bullet 1 wording revised from original to be more honest: "configured" instead of "implemented" schema evolution, "validated zero data loss with integration tests" instead of "ensuring zero data loss". Bullet 4 expanded to acknowledge dual-orchestrator reality.)

### 1.1 Dual purpose

| Purpose | Notes |
|---|---|
| Resume-defensible portfolio | Recruiter clones, runs, sees evidence; interviewer probes any claim → answer + code line cited |
| Personal trading research tool | Long-term low-cost ingestion of trades + bars for ongoing study |

The two purposes share **one codebase** with different deployment profiles. They do not conflict.

### 1.2 Success criteria

- Every claim in the four bullets traceable to a specific code path via README "Evidence Map"
- `docker compose up` produces a working stack within 5 minutes on a fresh clone
- AWS showcase profile deploys successfully via Terraform with screenshot evidence captured
- Databricks Free Edition deployment via Asset Bundle with Auto Loader screenshot evidence
- Personal long-term deployment running on Oracle Cloud Free Tier ($0/month) ingesting daily
- 80%+ unit test coverage on `pipelines/`
- 7 ADRs documenting key architectural decisions

### 1.3 Out of scope

- ML feature engineering on Gold layer (separate future portfolio project)
- L1 quote / bid-ask feeds (mentioned in README roadmap)
- Cross-exchange dedup / arbitrage analysis
- Real-time alerting beyond pipeline failures and quality threshold breaches
- Production-grade always-on Databricks deployment (Free Edition validation only)

---

## 2. Architecture

### 2.1 Conceptual data flow

```
WS Producers ──► landing/    ──► Bronze        ──► Silver       ──► Gold
(Binance,       (JSONL files,    (raw + raw_       (cleaned +       (1m/5m/1h/1d
 Alpaca,         atomic           json preserved,   normalized,       bars, daily
 Replay)         rotation)        partition by      partition by      volume profile,
                                  ingestion_date)   event_date,       market quality)
                                                    Z-order symbol)

                  ▲                  │                   │
                  │ schema           │ quality           │ quality
                  │ evolution        ▼ split             ▼ split
                  │              quarantine_         quarantine_
                  └──────────── trades / bars        bars
                                    │                   │
                                    └─── replay DAG ────┘
                                         (hourly,
                                          auto-recovery)
```

### 2.2 Bronze streaming + Silver/Gold batch

Bronze runs as **long-running Structured Streaming** (low-latency landing). Silver and Gold run as **Airflow-orchestrated micro-batch** (5-minute cadence) with `Trigger.AvailableNow`.

This split is deliberate: it lets each resume bullet point at a real architectural component (streaming → bullet 1; orchestration → bullet 4) rather than collapsing into one mode.

### 2.3 Deployment profiles

| Profile | Purpose | Cost | Lifecycle |
|---|---|---|---|
| `compose` (default) | Local dev + reviewer experience | $0 | Persistent on user machine |
| `aws-showcase` | Production-grade Terraform deployment | $5–15 per demo session | Ephemeral (`make aws-up` / `make aws-down`) |
| `personal-frugal` | Long-term personal use on Oracle Cloud | $0/month (Free Tier) | Always-on |
| `databricks` | Auto Loader + DBX validation via Asset Bundle | $0 (Free Edition, ~2h sessions) | On-demand |

All four profiles run the **same `pipelines/` Python package**. Differences are isolated in (a) `config/profiles/*.env` (env vars), (b) `infra/` (deployment IaC), (c) the dual `bronze_stream_reader` implementation that branches on `is_databricks()`.

### 2.4 Local docker-compose service topology

| Service | Role | Run mode |
|---|---|---|
| `minio` + `minio-init` | S3-compatible storage; bootstrap buckets | persistent |
| `postgres-airflow` | Airflow metadata DB | persistent |
| `airflow-init` | DB migrations | one-shot |
| `airflow-webserver` / `scheduler` / `triggerer` | Airflow control plane | persistent |
| `spark-master` + `spark-worker-{1,2}` | Spark Standalone cluster | persistent |
| `spark-bronze` | Long-running Bronze streaming job | persistent |
| `producer-binance` | Binance WS → JSONL files | persistent |
| `producer-alpaca` | Alpaca WS/REST → JSONL files | market hours; idle off-hours |
| `producer-replay` | Historical Parquet replay | profile-gated, on-demand |
| `metrics-publisher` | Delta → Prometheus bridge | persistent |
| `prometheus` + `grafana` | Observability stack | persistent |

13 services. Profile flags allow `--profile minimal` for CI subset (no Grafana/Prometheus).

---

## 3. Data Model (Medallion)

### 3.1 Bronze

| Table | Source | Partition | Z-order |
|---|---|---|---|
| `bronze.binance_trades` | Binance WS `trade` stream | `ingestion_date` | none |
| `bronze.alpaca_bars` | Alpaca Market Data | `ingestion_date` | none |
| `bronze.yfinance_history` | yfinance (manual backfill) | `ingestion_date` | none |

**Common columns:** `_ingest_ts`, `_source`, `_raw_json` (full payload preserved), `_file_path`, plus extracted core columns.

**Bronze is append-only and not Z-ordered**. It is allowed to contain malformed records; downstream (Silver) filters.

### 3.2 Silver

| Table | Content | Partition | Z-order |
|---|---|---|---|
| `silver.trades` | Cleaned, normalized trades from all sources | `event_date` | `symbol` |
| `silver.bars` | Cleaned, normalized bars (Alpaca direct + crypto rolled-up from `silver.trades`) | `event_date` | `symbol` |
| `silver.quarantine_trades` | Validation failures from trade pipeline | `quarantined_date` | none |
| `silver.quarantine_bars` | Validation failures from bar pipeline | `quarantined_date` | none |

**Unified Silver `trades` schema:** `event_ts`, `ingest_ts`, `event_date`, `source`, `asset_class`, `symbol`, `side`, `price` (DECIMAL(38,18)), `quantity` (DECIMAL(38,18)), `notional`, `trade_id`, `late_arrival_sec`.

**Quarantine schema:** Silver schema + `_error_code`, `_error_msg`, `_quarantined_ts`, `_replay_attempts` (int, max 3), `_raw_json`.

### 3.3 Gold

| Table | Content | Partition | Z-order |
|---|---|---|---|
| `gold.bars_1m` / `bars_5m` / `bars_1h` / `bars_1d` | OHLCV at multiple timeframes | `bar_date` | `symbol` |
| `gold.daily_volume_profile` | Per-symbol daily volume + VWAP + trade count | `bar_date` | `symbol` |
| `gold.market_quality` | Per-symbol per-hour stats: late-arrival ratio, quarantine ratio, price volatility | `metric_hour` | `symbol` |
| `gold.quarantine_bars` | Bar-level validation failures | `quarantined_date` | none |
| `gold.maintenance_metrics` | OPTIMIZE / VACUUM run history | `run_date` | none |

### 3.4 Lifecycle (retention + VACUUM)

| Layer | Retention | VACUUM threshold |
|---|---|---|
| Bronze | 90 days; rows older than 90 days moved to `bronze_archive/` (compressed Parquet, no Delta) | 7 days |
| Silver | 1 year | 7 days |
| Gold | Permanent (size small) | 30 days |
| Quarantine | 30 days, then archived to `quarantine_archive/` | 7 days |

---

## 4. Streaming Bronze Ingest

### 4.1 Producers

| Producer | Protocol | Schedule |
|---|---|---|
| `producer-binance` | Binance WS multi-symbol trade stream | 24/7 |
| `producer-alpaca` | Alpaca Market Data WS/REST | Market hours; idle off-hours |
| `producer-replay` | Historical Parquet → JSONL replay (configurable speedup) | On-demand |

**Common contract:**
- File rotation every 10 seconds OR 1000 records (whichever first)
- Atomic write: `.tmp.{name}` → `os.rename()` to final → Spark only sees complete files
- WS reconnect with exponential backoff (max 60s), in-memory buffer to absorb reconnection bursts
- **Acknowledged limitation:** WS messages dropped during reconnect window are not recoverable. Documented in README.

### 4.2 Bronze stream reader (dual implementation)

`pipelines/bronze/stream_reader.py` provides platform-neutral interface:

```python
def bronze_stream_reader(spark, source, landing_path, schema_path):
    if is_databricks():
        return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(landing_path))
    return (spark.readStream.format("json")
        .option("mergeSchema", "true")
        .schema(load_initial_schema(source))
        .load(landing_path))
```

Write path is uniform across both platforms: Delta sink + checkpoint location + 30s `processingTime` trigger + `partitionBy(ingestion_date)` + `mergeSchema=true`.

### 4.3 Checkpoints + exactly-once

- Each streaming query has a dedicated checkpoint dir under `s3a://lakehouse-meta/_checkpoints/<query>/`
- Delta sink ACID + checkpoint advancement → file-level exactly-once
- Nightly Airflow DAG syncs checkpoints to `_checkpoints_backup/{date}/` for corruption recovery

### 4.4 Failure mode matrix

| Failure | Zero-loss? | Mechanism |
|---|---|---|
| Producer container crash | ❌ (WS gap during reconnect) | docker `restart: always` + exponential backoff |
| Spark Bronze stream crash | ✅ | Checkpoint persisted; resume from last commit |
| Delta write mid-batch failure | ✅ | Delta transaction atomicity; checkpoint not advanced |
| Upstream schema change | ✅ | `mergeSchema` / Auto Loader `addNewColumns` |
| Storage backend unreachable | ✅ | Stream resumes on recovery |
| Checkpoint file corruption | ⚠ | Restore from nightly backup; `MERGE` upsert handles re-processing duplicates idempotently |

### 4.5 Schema evolution validation

`tests/integration/test_schema_evolution.py`:
1. Replay producer feeds v1 schema for 30s
2. Switch to v2 (adds `is_self_match` field) for another 30s
3. Bronze stream processes everything (`Trigger.AvailableNow`)
4. Assert: total count matches expected; v2-period records contain `is_self_match` parsed from `_raw_json`; Silver does not crash on unknown field; v1 fields still populated for v1-period rows

CI runs this on every push. README links to test code + green CI badge as the resume bullet 1 evidence.

---

## 5. Quality Framework

### 5.1 Framework abstraction

Custom lightweight framework (not Great Expectations / Pandera). ~200 lines.

```python
@dataclass(frozen=True)
class Rule:
    name: str
    error_code: str
    severity: Literal["error", "warning"]
    description: str
    predicate: Callable[[DataFrame], Column]  # boolean column

class QualityFramework:
    def __init__(self, rules: list[Rule]): ...
    def evaluate(self, df: DataFrame) -> DataFrame:
        """Adds _quality_failures: array<struct<error_code, error_msg>>."""
    def split(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Returns (passing, quarantined) split on any error-severity failure."""
```

Rules are **declarative data**, not arbitrary functions. Modules in `pipelines/quality/rules/` define rule lists per layer transition.

### 5.2 Layer-specific rule sets

**Bronze → Silver (trades):** `BR-001..007` covering price positivity, range sanity, quantity, symbol allowlist, event timestamp validity, trade_id presence, late arrival warning.

**Silver → Gold (bars):** `GR-001..003` covering bar consistency (high ≥ open/close, low ≤ open/close), volume non-negativity, key uniqueness.

Each rule module is independently unit-testable via small pandas DataFrame fixtures.

### 5.3 Quarantine boundaries

```
landing/  →  Bronze  ─[BR rules]─►  Silver / silver.quarantine_*
                              \
                               └─[GR rules]─►  Gold / gold.quarantine_*
```

**Bronze itself does not quarantine** (raw landing per Medallion convention). Filtering happens at the Bronze→Silver and Silver→Gold transitions.

### 5.4 Replay DAG semantics

`dags/quarantine_replay.py` runs hourly:

1. Read quarantine where `_replay_attempts < 3` and `_quarantined_ts` within last 24h
2. Re-load current rule set (rules may have been updated since last attempt)
3. `framework.split(eligible)` → `(now_passing, still_failing)`
4. `now_passing` → MERGE into target Silver/Gold table (idempotent on natural key)
5. `still_failing` → MERGE back to quarantine with `_replay_attempts += 1`
6. Records with `_replay_attempts == 3` → move to `quarantine_archive` + email alert
7. Write run metrics to `gold.maintenance_metrics`

### 5.5 "Automated recovery" evidence

`gold.market_quality.quarantine_rate` time series is the visual proof. README embeds the chart showing quarantine rate spike → rule fix deploy → next-hour replay catches up → rate returns to baseline. No manual data fixing required.

Integration test `test_quarantine_replay.py` deterministically demonstrates this loop in CI.

---

## 6. Delta Lake Optimization

### 6.1 Partition strategy

| Layer | Partition | Rationale |
|---|---|---|
| Bronze | `ingestion_date` | Streaming write spreads across day; OPTIMIZE/VACUUM per-day |
| Silver | `event_date` (event-time) | Query patterns: "BTC last N days" — event_date prune is the effective prune |
| Gold | `event_date` / `bar_date` | Same |

Day grain (not hour) chosen: 365 partitions/year vs 8760 — metadata vs prune tradeoff in our query patterns favors day.

**No partitioning by `symbol`** — high cardinality (10s–100s) would cause small-file storm. Symbol filtering handled by Z-order within partition.

### 6.2 Z-order strategy

Single column `symbol` on all Silver/Gold analytical tables. No multi-column Z-order — candidate second columns (`side`, `asset_class`) are low-cardinality (binary), no benefit.

Bronze and quarantine tables are not Z-ordered (Bronze is write-only; quarantine queries are time-window scans).

### 6.3 Compaction (two-tier)

**Tier 1 — write-side auto:** Delta table properties `delta.autoOptimize.optimizeWrite=true` and `delta.autoOptimize.autoCompact=true`. Reduces 30s-trigger small-file generation at write time.

**Tier 2 — scheduled OPTIMIZE (primary):**

| DAG | Schedule | Scope | Operation |
|---|---|---|---|
| `optimize_hot` | every 6 hours | last 2 days of partitions | `OPTIMIZE` (compact only) |
| `optimize_zorder_nightly` | daily 02:00 UTC | all Silver/Gold partitions | `OPTIMIZE … ZORDER BY (symbol)` |
| `vacuum_nightly` | daily 03:00 UTC | per-table retention windows | `VACUUM … RETAIN N HOURS` |

`vacuum_nightly` uses cross-DAG dependency on `optimize_zorder_nightly` (Airflow Datasets) to avoid VACUUM running before compaction.

### 6.4 Benchmark plan

Standalone reproducible benchmark in `benchmarks/run_optimization_benchmark.py`:

- Replay producer pre-loads ~1B trade records spanning 30+ symbols × multiple days
- Three states measured: (A) post-ingest no optimize; (B) post-OPTIMIZE compact only; (C) post-OPTIMIZE + ZORDER
- 4 standard queries (Q1: single-symbol 24h scan; Q2: single-symbol 7d hourly aggregation; Q3: 5-symbol cross-section last hour; Q4: full-table count by symbol) × 5 runs each, median reported
- Outputs: `benchmarks/results.md` (auto-generated table), `benchmarks/charts/optimization_comparison.png` (matplotlib bar), `benchmarks/raw/{ts}.json` (raw data)
- README embeds the result table + reproduction command

CI runs a 10M-row variant on every push to keep the script working.

---

## 7. Orchestration

### 7.1 DAG inventory (Airflow, OSS profile)

| DAG | Schedule | Purpose |
|---|---|---|
| `silver_pipeline` | `*/5 * * * *` | Bronze → Silver transformation + quality split |
| `gold_aggregations` | Dataset-triggered (on Silver outlets) | Silver → Gold roll-ups |
| `quarantine_replay` | `0 * * * *` | Hourly replay across all layers |
| `optimize_hot` | `0 */6 * * *` | Recent-partition compaction |
| `optimize_zorder_nightly` | `0 2 * * *` | Full Z-order rebuild |
| `vacuum_nightly` | `0 3 * * *` (after zorder) | Retention enforcement |
| `historical_backfill` | manual | yfinance historical pull |
| `schema_evolution_demo` | manual | Demo orchestration for video capture |

### 7.2 Dependency-aware scheduling — Airflow Datasets

Modern Airflow 2.4+ Datasets used for cross-DAG dependencies:

```python
silver_trades_ds = Dataset("delta://lakehouse/silver/trades")

# silver_pipeline declares outlet
SparkSubmitOperator(..., outlets=[silver_trades_ds])

# gold_aggregations subscribes
@dag(schedule=[silver_trades_ds, silver_bars_ds])
def gold_aggregations(): ...
```

This is declarative producer/consumer scheduling, not procedural ExternalTaskSensor polling. Direct match for resume bullet 4 "dependency-aware scheduling".

### 7.3 Failure alerting (Slack)

```python
default_args = {
    "on_failure_callback": alert_on_failure,  # POST to Slack webhook
    "on_sla_miss_callback": alert_on_sla_miss,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=3),
}
```

Three trigger types: task failure (red), SLA miss (yellow), data quality threshold breach (yellow). Slack default; SMTP fallback documented.

### 7.4 Containerization & dev/prod parity

| Concern | dev (compose) | aws-showcase | personal-frugal | databricks |
|---|---|---|---|---|
| Storage | MinIO | S3 | OCI Object Storage | DBFS / Unity Catalog |
| Spark | Standalone in compose | EMR on EC2 (3-node) | Standalone in compose on Oracle VM | Databricks Job clusters |
| Airflow | LocalExecutor + Postgres | LocalExecutor + RDS | LocalExecutor + local Postgres | Replaced by Databricks Jobs |
| Same Docker images | ✅ | ✅ | ✅ | N/A (DBX uses notebooks + wheel) |
| Same `pipelines/` package | ✅ | ✅ | ✅ | ✅ (as wheel library) |

The "reproducible deployment" claim rests on: same images + same Python package + only env vars differ.

---

## 8. Hero Artifacts

### 8.1 Grafana dashboard

One dashboard (`dashboards/lakehouse-overview.json`, auto-provisioned) with rows:
- Stream Health (producer rates, ingest lag P50/P95/P99)
- Pipeline Health (DAG durations, SLA hit rate, quarantine ratios per layer)
- Data Quality (per-rule violation counts, late-arrival distribution, replay recovery curve)
- Storage Health (table sizes, file counts, Z-order coverage)
- Cost (showcase profile only)

Data sources: Prometheus (scrapes producer/Spark/Airflow exporters) + `metrics-publisher` (Python service that periodically queries Delta and emits Prometheus metrics — avoids fragile Trino/DuckDB Grafana plugins).

README embeds a 15–20s GIF of the dashboard.

### 8.2 Z-order benchmark report

See §6.4. Output is `benchmarks/results.md` table + chart, embedded in main README.

### 8.3 AWS deployment evidence

`infra/terraform/aws-showcase/` is a full production-grade Terraform with:
- VPC + private/public subnets + NAT GW + S3 Gateway Endpoint
- IAM roles (least privilege) + GitHub OIDC for CI
- SSM Parameter Store (not Secrets Manager — cost)
- EMR on EC2 (3-node, ephemeral)
- ECS Fargate tasks (Airflow + producers)
- CloudWatch log groups + alarms

Make targets: `aws-up` / `aws-demo` / `aws-benchmark` / `aws-screenshots` / `aws-down` / `aws-cost`.

Captured artifacts in `docs/showcase/aws-run-{ts}/`:
- Architecture diagram (drawio + PNG)
- CloudWatch screenshots
- EMR Step UI screenshots
- S3 console screenshot
- Grafana dashboard exports
- 5-min Loom walkthrough video link

### 8.4 README structure

The README itself is the primary hero artifact. Structure:
1. Banner + 4 CI badges
2. 30-second pitch + architecture diagram (top-of-fold)
3. Quick Start (5 commands)
4. Live dashboard preview GIF
5. **Resume Claims → Evidence Map** (the critical anchor)
6. Detailed sections (data sources, pipeline, quality, optimization, orchestration, AWS, Oracle, Databricks)
7. Roadmap (bid/ask, ML extension)
8. Tech stack + author

The Evidence Map is a table with columns `Claim | Where to Verify` linking each resume bullet to specific code paths, tests, screenshots, or charts. This is the core deliverable that turns the codebase into a verifiable resume artifact.

### 8.5 Walkthrough videos

- 5-min main walkthrough (compose + e2e demo + AWS console + Grafana)
- 2-min Databricks-specific walkthrough (DAB deploy + Auto Loader run + schema evolution)
- Hosted on Loom (free), linked in README

---

## 9. Repository Layout & Dev Workflow

### 9.1 Top-level tree

```
financial-lakehouse/
├── README.md                  # Hero document
├── pyproject.toml + uv.lock
├── Makefile
├── docker-bake.hcl
├── docker-compose.yml + .minimal.yml
├── pipelines/                 # PySpark business core (90% of code)
│   ├── config/ schemas/{bronze,silver,gold}/ sources/
│   ├── bronze/ silver/ gold/ quality/ maintenance/
├── producers/                 # WS/REST producers
├── dags/                      # 8 Airflow DAGs + _common/
├── metrics_publisher/         # Delta → Prometheus bridge
├── docker/                    # Per-service Dockerfiles
├── infra/
│   ├── terraform/aws-showcase/   # EMR on EC2 (primary)
│   ├── ansible/personal-frugal/
│   └── databricks/            # Asset Bundle
├── observability/{prometheus,grafana}/
├── benchmarks/
├── tests/{unit,integration,dag}/
├── scripts/
├── docs/{architecture,showcase,decisions,dev}/
├── config/profiles/
└── .github/workflows/
```

### 9.2 Toolchain

- `uv` (package manager — Rust, ~100x pip)
- `ruff` (lint + format, replaces 4 tools)
- `mypy --strict` on `pipelines/` only
- `pytest` + `pytest-spark` for integration
- `pre-commit` hooks
- Plain markdown docs (no static site generator)

### 9.3 Testing tiers

| Tier | Path | Runtime | Coverage target |
|---|---|---|---|
| Unit | `tests/unit/` | <30s | 80%+ on `pipelines/` |
| Integration | `tests/integration/` | <5min | Key bullet evidence tests |
| DAG static | `tests/dag/` | <10s | All DAGs import + structure validation |

Critical integration tests (each ⇄ a resume bullet):
- `test_schema_evolution.py` ⇄ bullet 1
- `test_checkpoint_recovery.py` ⇄ bullet 1
- `test_quarantine_replay.py` ⇄ bullet 2
- `test_e2e_pipeline.py` ⇄ bullets 1+2
- `test_z_order_benefit.py` ⇄ bullet 3
- `test_dataset_triggers.py` ⇄ bullet 4

### 9.4 CI matrix (GitHub Actions)

| Workflow | Trigger | Time | Cost |
|---|---|---|---|
| `lint-test.yml` (unit + lint + typecheck) | every push | 3 min | free |
| `integration-test.yml` (PySpark + MinIO compose) | every push | 8 min | free |
| `dag-validate.yml` | every push | 1 min | free |
| `build-images.yml` (buildx → GHCR) | push to main | 12 min | free |
| `validate-terraform.yml` | infra/ changes | 1 min | free |
| `benchmark-small.yml` (10M rows) | weekly + manual | 15 min | free |
| `deploy-aws-showcase.yml` (OIDC, manual) | manual | 30+ min | $5–15/run |

### 9.5 ADRs

`docs/decisions/` contains 7 ADRs (1–2 pages each):

| # | Topic |
|---|---|
| 0001 | Medallion vs Kappa Architecture |
| 0002 | Custom Quality Framework vs Great Expectations / Pandera |
| 0003 | EMR on EC2 vs EMR Serverless |
| 0004 | Spark Standalone (compose) vs local mode |
| 0005 | Airflow Datasets vs ExternalTaskSensor |
| 0006 | Bronze accept-all + Silver MERGE dedup |
| 0007 | Custom metrics-publisher vs Trino-on-Delta plugin |

---

## 10. Databricks Compatibility

### 10.1 Asset Bundle (DAB)

Deployment via `databricks bundle deploy`. Structure under `infra/databricks/`:
- `databricks.yml` — bundle entry + targets
- `resources/jobs/*.yml` — Jobs mirroring Airflow DAGs (Bronze streaming, Silver pipeline, Gold aggregations, quarantine replay, maintenance)
- `resources/schemas/lakehouse.yml` — Unity Catalog schemas
- `resources/clusters/job_cluster.yml` — shared cluster config
- `notebooks/` — thin notebook entry points (10 lines each, import from wheel)
- `libs/` — built `pipelines-1.0.0-py3-none-any.whl` (CI artifact)

### 10.2 Shared wheel approach

`pipelines/` builds as standard Python wheel. Databricks notebooks install the wheel as a Job library and import normally:

```python
%pip install /Workspace/Shared/libs/pipelines-1.0.0-py3-none-any.whl
from pipelines.bronze.stream_reader import bronze_stream_reader, write_bronze
```

`is_databricks()` (env detection via `DATABRICKS_RUNTIME_VERSION`) routes to Auto Loader. Notebooks remain thin to avoid the "code in notebook" anti-pattern.

### 10.3 Auto Loader features verified

| Feature | Used | Why it matters |
|---|---|---|
| `cloudFiles.format` | ✅ | required |
| `cloudFiles.schemaLocation` | ✅ | persistent schema state |
| `cloudFiles.schemaEvolutionMode = addNewColumns` | ✅ | bullet 1 schema evolution |
| `cloudFiles.includeExistingFiles` | ✅ | initial backfill |
| **File notification mode** (S3 Events + SNS + SQS) | ✅ | scalability + cost vs directory listing |

DAB provisions the SNS/SQS for file notification.

### 10.4 Databricks Jobs vs Airflow

The Databricks profile uses **Databricks Jobs** (with task dependencies) for orchestration, not Airflow. Airflow is the OSS-profile orchestrator. Resume bullet 4 wording acknowledges both.

This is a credible platform-appropriate choice, defensible in interview: Databricks Jobs is the native scheduler with dependency support, alerting, and lineage; running Airflow on Databricks is uncommon and unjustified for a lakehouse-native workload.

### 10.5 Free Edition usage

Databricks Free Edition is sufficient for:
- DAB deployment + workspace UI screenshots
- Auto Loader streaming with small data
- Schema evolution validation
- 2-min walkthrough video capture

It is **not** sufficient for the 1B benchmark (small cluster). The benchmark stays on EMR on EC2 (§6.4).

Total Databricks active session time estimate: 1–2 hours across the project lifecycle, all within Free Edition.

### 10.6 Captured artifacts

`docs/showcase/databricks/`: bundle deploy output, workspace overview, Bronze job running with Auto Loader metrics, schema evolution mid-flight, job dependency graph, Delta Catalog browser, post-tear-down state. Plus 2-min Loom video.

---

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Spark Standalone setup debugging eats >2 weekends | Schedule slip | ADR documents fallback to local mode; budget includes 1-weekend buffer |
| AWS cost overrun (forgotten resources) | Wallet pain | `make aws-down` mandatory step; Terraform state-tracked destroy; weekly `make aws-cost` reminder; CloudWatch billing alarm at $25 |
| Databricks Free Edition workspace expires | Loss of deployed state | All deployment via DAB; redeploy in <30 min from repo |
| Schema evolution test flakiness | CI red | Deterministic test data (replay producer with fixed timing); fixed schema versioning |
| Crypto WS rate limits / disconnects | Documented gap | README disclosure + reconnect with exponential backoff |
| 80% coverage unreachable on PySpark code | Quality target miss | Focus 80%+ on pure-Python (`quality/`, `schemas/`, `config/`); integration tests cover Spark code paths |
| Oracle Cloud Free Tier ARM compatibility | Personal-frugal blocker | All Docker images built `linux/arm64` via buildx; CI matrix includes ARM build |

---

## 12. Time Budget & Phasing

| Phase | Scope | Effort |
|---|---|---|
| 1 — Foundation | Repo skeleton, compose stack, MinIO, basic producers, Bronze landing | 1.5 weekends |
| 2 — Pipeline core | Silver/Gold transforms, quality framework, quarantine + replay | 2 weekends |
| 3 — Optimization | Z-order, compaction, maintenance DAGs, benchmark | 1.5 weekends |
| 4 — Hero polish | Grafana, ADRs, README, walkthrough video | 1 weekend |
| 5 — AWS showcase | Terraform, EMR on EC2, deploy & capture | 1.5 weekends |
| 6 — Databricks | Asset Bundle, notebooks, deploy & capture | 1.5 weekends |
| 7 — Personal frugal | Ansible to Oracle Cloud, long-term setup | 0.5 weekends |
| **Total** | | **~9.5 weekends (~2.5 months calendar)** |

Phases 1–3 must be sequential. Phases 4–7 can interleave or run partially in parallel.

---

## 13. Resume Bullets → Evidence Map (preview)

(Final form lives in README §6 with concrete file paths and links once code exists.)

| Claim | Evidence Type |
|---|---|
| Auto Loader on Databricks | `pipelines/bronze/stream_reader.py` (cloudFiles branch) + DBX notebook screenshot |
| Structured Streaming + checkpoint fault tolerance | Stream reader code + `test_checkpoint_recovery.py` + failure matrix in README |
| Schema evolution + zero data loss | `test_schema_evolution.py` + CI badge + README explanation |
| Quarantine-and-replay across Bronze/Silver/Gold | `pipelines/quality/framework.py` + `dags/quarantine_replay.py` + recovery chart |
| Bronze/Silver/Gold Medallion | Architecture diagram + `pipelines/schemas/{bronze,silver,gold}/` |
| Z-ordering on high-cardinality | `dags/optimize_zorder_nightly.py` + `benchmarks/results.md` |
| Automated compaction | `dags/optimize_hot.py` + file-count metric chart |
| Time-series partition strategy | Architecture §3 + `pipelines/schemas/partition_spec.py` |
| Airflow + Databricks Jobs orchestration | `dags/` + `infra/databricks/resources/jobs/` + Datasets code |
| Failure alerting | `dags/_common/callbacks.py` + Slack alert screenshot |
| Docker reproducible across environments | `docker-compose.yml` + GHCR images + Terraform + Ansible |

---

## 14. Open Questions for Future Iterations

- L1 quotes (bid/ask) ingestion + spread analysis — Roadmap candidate after MVP
- ML feature engineering on Gold — Separate portfolio project
- Multi-region failover for AWS showcase — Not justified at portfolio scale
- Cross-source dedup / arbitrage features — Not in resume scope

# Plan 1 Closeout — Work Report

**Date:** 2026-05-04
**Author:** Claude Opus 4.7 (1M context), executing autonomously after the user delegated full ownership at the end of the brainstorm.
**Branch:** `main` (per project convention; no worktree was used).
**Final commit on `origin/main`:** `9e97d46`.

This report is intended to be reviewed by Opus before deciding the next development step.

---

## 1. Executive Summary

The five-deliverable Plan 1 (MVP) closeout sub-spec was executed end-to-end with zero
external spend. Plan 1's four committed resume bullets now back themselves with
machine-verifiable evidence (unit tests, integration tests, CI badges, runtime artifacts)
and one ~4-hour live-feed soak captured against the Binance public WebSocket. CLAUDE.md is
updated; Plan 1 is marked Done; the recommended next step is Plan 3 (Databricks Asset
Bundle, Free Edition, $0).

Three things happened mid-execution that were not in the original brainstorm and deserve
explicit review (§7).

---

## 2. Scope That Was Agreed

The brainstorm at `docs/superpowers/specs/2026-05-03-plan1-closeout-design.md` locked the
following five deliverables. The user explicitly accepted all five in the AskUserQuestion
gate, including the soak which carries a known overlap risk with future Plan 3 Auto Loader
work.

| # | Deliverable | Resume bullet it strengthens |
|---|---|---|
| 1 | Three Mermaid architecture diagrams + embedding in README and overview doc | All four (visual anchor) |
| 2 | Four GHA workflows hardened to green badges | All four (signal quality) |
| 3 | Slack failure-alerting unit test + captured POST body + manual screenshot recipe | Bullet 4 |
| 4 | Compose-level historical replay end-to-end demo + integration test | Bullet 2 |
| 5 | 4-hour Binance public-WS soak with evidence | Bullet 1 (drops "no live soak" caveat) |

Non-goals (out of scope, deliberately not done):

- Alpaca soak (REST polling does not strengthen "real-time" wording)
- GHCR image push + `docker compose pull` switch (own future sub-spec)
- Plan 2 / 3 / 4 work
- 24h+ soak (WSL host-sleep risk vs. marginal evidence)
- Pipeline functional changes (Bronze/Silver/Gold logic frozen)

---

## 3. Plan + Spec Artifacts

| Artifact | Path | Commit |
|---|---|---|
| Design spec (the WHAT and WHY) | `docs/superpowers/specs/2026-05-03-plan1-closeout-design.md` | `69109eb` |
| Implementation plan (14 tasks) | `docs/superpowers/plans/2026-05-03-plan1-closeout.md` | `ff3c9c4` |

Both are committed and pushed.

---

## 4. Per-Deliverable Outcome

### 4.1 Mermaid Architecture Diagrams (Tasks 1-2)

**New files (commit `49902f3`):**

- `docs/architecture/diagrams/data-flow.mmd` — producers → MinIO landing → Bronze →
  Silver → Gold, including the replay landing prefix.
- `docs/architecture/diagrams/compose-topology.mmd` — service graph for the local
  compose stack, including streaming and live profiles.
- `docs/architecture/diagrams/airflow-datasets.mmd` — DAG dependency graph via
  Airflow Datasets, including the cross-DAG ordering between optimize and vacuum.

**Embedded (commit `0b035db`):**

- `README.md` — four GHA badges below the H1 + the data-flow diagram immediately above
  the `## Readiness` section.
- `docs/architecture/overview.md` — all three diagrams as inline ` ```mermaid ` blocks
  with H3 captions.

**Verification:** GitHub renders Mermaid blocks inline — visual confirmation by opening
the README on github.com after push.

### 4.2 GHA Workflow Hardening (Tasks 9-10)

**Edited (commit `89da8cb`):**

- `.github/workflows/lint-test.yml` — adds ruff format check, mypy, uv cache, Python 3.11
  setup, per-ref concurrency group.
- `.github/workflows/dag-validate.yml` — adds airflow extra explicitly + same scaffolding.
- `.github/workflows/integration-test.yml` — adds Java 17 (Temurin) so PySpark/Delta
  start cleanly + same scaffolding.
- `.github/workflows/build-images.yml` — adds buildx GHA cache + runs on PR and main.
- `.github/workflows/benchmark-small.yml` — switches from per-push trigger to
  `workflow_dispatch` only.

**Verification:** All four target workflows are green on commit `9e97d46`:

```
build-images     9e97d46 completed success
integration-test 9e97d46 completed success
lint-test        9e97d46 completed success
dag-validate     9e97d46 completed success
```

Badge SVG URLs all return HTTP 200 with `image/svg+xml`. **No iteration was needed** —
all four workflows went green on the first push after the local pre-flight passed.

### 4.3 Slack Failure-Alerting Smoke (Tasks 3-4)

**New tests (commit `708afcb`):** `tests/unit/test_callbacks.py` with three cases:

1. `test_alert_on_failure_posts_expected_body_when_webhook_set` — patches `urlopen`,
   sets `ALERTS__SLACK_WEBHOOK_URL`, asserts request URL, JSON body schema, and that
   `dag_id`/`task_id` render into the message text.
2. `test_alert_on_failure_noop_when_webhook_unset` — no env var, no request issued, no
   exception raised.
3. `test_alert_on_sla_miss_returns_none` — guards the placeholder against future
   signature drift.

All three pass locally and in CI's `lint-test.yml`.

**New scripts + docs (commit `f2c8f68`):**

- `scripts/send_test_slack_alert.py` — one-shot CLI that reads
  `ALERTS__SLACK_WEBHOOK_URL`, posts the same body shape the live callback produces,
  prints HTTP status. For the user to run once when they have a real webhook URL.
- `docs/showcase/slack/airflow-failure-example.json` — the authoritative POST body the
  callback emits, captured programmatically (not synthesized prose) by patching
  `urlopen` and dumping the captured request. **This is the primary defensible artifact
  for the bullet.**
- `docs/showcase/slack/README.md` — explains both the JSON artifact (machine-verifiable)
  and the manual screenshot capture procedure (human-readable companion).

**Substitution disclosed:** the assistant cannot operate a browser, so the visual
screenshot is left as a documented manual step. The JSON body + the unit test that runs
on every CI build are sufficient to defend the bullet under interview scrutiny.

### 4.4 Compose-Level Historical Replay End-to-End Demo (Tasks 5, 5b, 6, 7, 11)

**New files:**

- `scripts/seed_replay_parquet.py` — generates a deterministic 50-row Parquet under
  `data/replay/` (gitignored). Column names + types match `BINANCE_TRADE_SCHEMA` exactly
  (event_type, event_time, symbol, trade_id, price as string, quantity as string,
  trade_time, buyer_is_maker) so Spark's JSON reader fully populates Bronze (commit
  `b833685`).
- `scripts/bronze_replay_count.py` — mirrors `bronze_count.py` for the dedicated
  `binance_replay_trades` Delta table (commit `b833685`).
- `jobs/bronze_replay_binance.py` — mirrors `jobs/bronze_binance_stream.py` for the new
  `replay/binance` source path; writes a separate Bronze Delta target with a separate
  checkpoint location (commit `32b8f89`).
- `tests/integration/test_replay_compose.py` — end-to-end test on local fs (no MinIO
  required): seeds Parquet → ReplayProducer writes JSONL → Spark applies
  `BINANCE_TRADE_SCHEMA` → Bronze Delta → asserts count + idempotency under re-run
  (commit `165f716`).

**Modified files:**

- `producers/replay.py` — `ReplayProducer.__init__` now accepts `landing_root` as either
  `str | Path` and an optional `writer: JsonlWriter | None`. When the caller passes an
  `s3://` or `s3a://` URL and no explicit writer, ReplayProducer auto-builds an
  `S3JsonlWriter` from environment variables; otherwise it builds an `AtomicJsonlWriter`
  pointing at the local Path (commit `32b8f89`). **Backward-compatible:** the existing
  `tests/unit/test_replay_producer.py` and the local-fs path in `scripts/replay_demo.py`
  still pass without change.
- `scripts/replay_from_history.py` — `--landing-root` now accepts either a Path or a
  s3a URL via a small type-coercer (commit `32b8f89`).
- `Makefile` — new targets `seed-replay`, `replay-bronze-demo`, `bronze-replay-count`,
  `soak-binance` (commit `ec73033`).

**End-to-end runtime evidence (commit `5d50a37`, `docs/showcase/replay/2026-05-03-replay-bronze-demo.md`):**

```
make reset && make deploy-local && make replay-bronze-demo && make bronze-replay-count

Spark structured streaming:
  numInputRows: 50
  processedRowsPerSecond: 3.24
  sink: DeltaSink[s3a://lakehouse/bronze/binance_replay_trades]

=== BRONZE REPLAY COUNT: 50 records ===
```

50 deterministic rows survived the full chain: host-side ReplayProducer → MinIO
`landing/replay/binance/` → Spark structured streaming with `BINANCE_TRADE_SCHEMA` →
Bronze Delta → count.

**Test evidence:** `tests/integration/test_replay_compose.py` passes locally and in
`integration-test.yml` on every push.

### 4.5 4-Hour Binance Soak (Tasks 8, 12, 13)

**Procedure scripted (commit `e80a86b`):** `scripts/run_binance_soak.sh` runs an
unattended soak with configurable `SOAK_DURATION_SEC` (default 14400 = 4h) and
`SOAK_INTERVAL_SEC` (default 1800 = 30 min). Each interval captures one snapshot to
`docs/showcase/soak/_raw/snapshot-N-<ts>.txt` (gitignored). Snapshot contents:

- `make metrics-snapshot` — Prometheus gauges including
  `lakehouse_bronze_binance_records_total`.
- `docker compose logs --tail 80 producer-binance` — captures any error/reconnect
  markers (the producer is silent-by-design otherwise).
- `docker compose logs --tail 40 spark-bronze` — captures structured-streaming
  micro-batch progress dumps.

**Window:**

- Start: `2026-05-03T16:47:38Z`
- End: `2026-05-03T20:41:59Z`
- Duration: 3h 54m 21s (~6 minutes shorter than nominal 4h due to 30-min interval
  rounding; the script captured all 9 planned snapshots).

**Result headline (commit `9bf4788`,
`docs/showcase/soak/2026-05-03-binance-soak.md`):**

| Snapshot | Timestamp UTC | Bronze records | Δ |
|---:|---|---:|---:|
| 0 | 16:47:38 | 215,689 | — |
| 1 | 17:17:06 | 271,545 | +55,856 |
| 2 | 17:46:33 | 332,093 | +60,548 |
| 3 | 18:15:55 | 370,183 | +38,090 |
| 4 | 18:45:10 | 404,378 | +34,195 |
| 5 | 19:14:26 | 432,710 | +28,332 |
| 6 | 19:43:43 | 478,675 | +45,965 |
| 7 | 20:12:50 | 521,008 | +42,333 |
| 8 | 20:41:54 | 646,795 | +125,787 |

- Total over window: **+431,106 records** in ~3h 54min.
- Average rate: **~30.7 records/sec** sustained against three Binance public-WS symbols
  (BTCUSDT, ETHUSDT, SOLUSDT).
- Bronze count is **monotonically non-decreasing** across every interval — the headline
  soak invariant.
- Delta transaction log progressed past `version=468` during the window, confirming the
  Structured Streaming consumer was actively committing micro-batches.
- No producer or stream restart events observed in the log tails.

**Substitution disclosed:** the assistant cannot operate a browser, so the Grafana
panel screenshots called for in the brainstorm were replaced by per-snapshot text
dumps of `make metrics-snapshot` (which reads the same Prometheus gauges Grafana
displays). `docs/showcase/soak/README.md` documents the manual Grafana capture as
optional follow-up. The text-table evidence is the resume-defensible artifact and
survives without a browser tool.

---

## 5. Verification (How Each Claim Is Backed)

### 5.1 Local

```bash
make validate-release   # PASSED in 9m17s on commit 9e97d46
# 51 tests passed (unit + dag + integration)
# ruff check . — All checks passed!
# ruff format --check . — 128 files already formatted
# mypy — Success: no issues found in 65 source files
# docker compose config --quiet — exit 0
```

```bash
make deploy-local       # PASSED — stack healthy
make e2e-local          # PASSED — exit 0
make replay-bronze-demo # PASSED — 50 records end-to-end
make bronze-replay-count # === BRONZE REPLAY COUNT: 50 records ===
```

### 5.2 CI

Four target workflows green on `main` at commit `9e97d46`:

```
lint-test        success
dag-validate     success
integration-test success
build-images     success
```

Badge SVG URLs return HTTP 200; visible at the top of the README.

### 5.3 Runtime Artifacts

- 50 records: `s3a://lakehouse/bronze/binance_replay_trades` (replay path)
- 646,795 records: `s3a://lakehouse/bronze/binance_trades` at soak end (live path)
- 9 raw snapshots: `docs/showcase/soak/_raw/` (gitignored)
- Consolidated soak doc: `docs/showcase/soak/2026-05-03-binance-soak.md`
- Replay demo log: `docs/showcase/replay/2026-05-03-replay-bronze-demo.md`
- Slack POST body: `docs/showcase/slack/airflow-failure-example.json`

---

## 6. Files Changed Summary

**16 commits this session, all on `origin/main`:**

```
9e97d46 fix: handle missing Delta tables in count scripts; mark Plan 1 Done
9bf4788 docs: capture 4h Binance public WS soak evidence
1595bfa fix: drop bronze-count from soak snapshots to avoid Spark contention
5d50a37 docs: capture compose-level replay-bronze demo evidence
89da8cb ci: harden GHA workflows for green badges
e80a86b feat: add 4h Binance soak procedure and README
ec73033 feat: add Makefile targets for replay demo and soak
165f716 test: cover replay-to-Bronze chain end-to-end
32b8f89 feat: route ReplayProducer to S3JsonlWriter for s3a landing roots
b833685 feat: add replay Parquet seed and Bronze replay count helper
f2c8f68 feat: add Slack alert sender script and captured POST body
708afcb test: cover Slack failure callback
0b035db docs: embed architecture diagrams and CI badges in README
49902f3 docs: add Mermaid architecture diagrams
ff3c9c4 docs: add Plan 1 closeout implementation plan
69109eb docs: add Plan 1 closeout design spec
```

---

## 7. Decisions Made Mid-Execution Without Explicit Re-Approval

The user delegated `全权负责`, so the following calls were made and are flagged here for
review.

### 7.1 Soak script switched from `bronze-count` to `metrics-snapshot` for the headline metric

**Why it was needed:** The first soak attempt hung for ~50 minutes on the very first
snapshot. Root cause: `make bronze-count` submits a fresh `spark-submit` to the
standalone cluster, but `spark-bronze` (the continuous streaming consumer required for
the soak to be valid) holds all worker resources. The new app waited indefinitely for
executors that would never become available.

**Fix (commit `1595bfa`):** snapshot now relies on `make metrics-snapshot`, which uses
`metrics_publisher` to read Delta transaction logs directly via `boto3` (no Spark
cluster contention). The same gauge value is captured. The script also now captures
`spark-bronze` log tails per snapshot so stream-stall events would be visible.

**Why this is honest:** the metric source changed from "Spark count" to "Delta
transaction-log derived count." Both should agree exactly because Delta record counts
are stored in the log's `add` actions. The substitution is faithful to the original
intent.

### 7.2 Four count scripts patched for `AnalysisException`

**What was found:** `scripts/{bronze,silver,gold,bronze_replay}_count.py` all caught
only `Py4JJavaError` for missing Delta tables. PySpark 3.5 raises
`pyspark.errors.exceptions.captured.AnalysisException` with `PATH_NOT_FOUND` for
missing paths, which propagated unhandled and broke `make e2e-local` on a freshly-reset
MinIO whenever any of the optional tables (`alpaca_bars`, `silver/bars`, `silver/quarantine_bars`,
`gold/bars_5m`) had not yet been seeded.

**Why this was a hidden bug:** prior progress notes ran `make e2e-local` against MinIO
state retained from earlier runs that *did* include alpaca-bars seeding. A genuinely
clean `make reset && make deploy-local && make e2e-local` had not been verified before.

**Fix (commit `9e97d46`):** all four scripts now catch both `Py4JJavaError` and
`AnalysisException` and recognize `PATH_NOT_FOUND` alongside `DELTA_TABLE_NOT_FOUND`.

**Why this is in scope:** the closeout sub-spec's success criteria include
`make deploy-local && make e2e-local` still passing; the only way to satisfy that on a
clean MinIO is to fix this. The fix is in count *scripts* (evidence helpers), not in
pipeline code, so it does not violate the "no pipeline functional changes" non-goal.

### 7.3 Skipped per-task review gates and `finishing-a-development-branch` ceremony

**Why:** the user's explicit instruction was `我只在乎结果` and `遇事不决动用你的最佳判断`.
This overrode the `subagent-driven-development with explicit per-task review gates`
default in CLAUDE.md and the `superpowers:finishing-a-development-branch` ceremony at
the end of `executing-plans`. All commits went directly to `main`, matching prior
project convention from the git log.

---

## 8. Honest Gaps and Manual Follow-ups

These are documented in the relevant showcase READMEs and are explicitly NOT claimed in
the resume bullets:

| Gap | Where it is documented | Why it was left manual |
|---|---|---|
| Visual Slack screenshot | `docs/showcase/slack/README.md` | Assistant cannot operate a browser; user has access to a Slack workspace. JSON body covers the machine-verifiable claim. |
| Visual Grafana screenshot | `docs/showcase/soak/README.md` | Same browser limitation. Per-snapshot text-table metrics over time are the substitute. |
| Long-running (24h+) soak | Out of scope by user choice during brainstorm | WSL2 host-sleep risk vs. marginal evidence does not justify the schedule risk. |

---

## 9. What Was NOT Done (Deliberately)

- Plan 2 / 3 / 4 work — out of scope.
- GHCR image push + `docker compose pull` switch — own future sub-spec because it
  changes the deployment model.
- Alpaca soak — does not strengthen "real-time" wording vs. Binance WS.
- Older progress-report cleanup — low-value housekeeping; can ride a separate trivial
  commit.

---

## 10. Recommendation for Next Step

Plan 1 is Done. The two remaining $0 levers that strengthen the resume title (`Databricks/Spark/AWS`)
are Plan 3 (Databricks Asset Bundle, Free Edition) and Plan 4 (Oracle Cloud Free Tier).
Plan 2 (AWS) costs ~$10-30 per demo cycle and has no $0 substitute.

**Recommendation:** brainstorm Plan 3 next.

- Cost: $0 (Databricks Free Edition).
- Resume yield: restores `Databricks` keyword in the bullet title and adds Auto Loader
  evidence (which the design spec calls out as the production schema-evolution runtime).
- Risk: low — the existing `pipelines/` package is wheel-buildable; the project's
  Bronze stream reader has a documented dual-implementation branch for Auto Loader vs.
  OSS file streaming, so the integration boundary is already designed.

After Plan 3, evaluate whether Plan 4 (Oracle Free Tier always-on) is worth doing
before Plan 2 (AWS short-lived showcase). The order Plan 1 → Plan 3 → Plan 4 → Plan 2
keeps all $0 levers in front of the only paid one.

---

## 11. Files an Opus Reviewer Should Open

To audit this work without re-running anything:

1. `docs/superpowers/specs/2026-05-03-plan1-closeout-design.md` — the agreed scope.
2. `docs/superpowers/plans/2026-05-03-plan1-closeout.md` — the executed plan.
3. `docs/showcase/soak/2026-05-03-binance-soak.md` — strongest piece of evidence (live
   feed, monotonic Bronze growth, ~431k records).
4. `docs/showcase/replay/2026-05-03-replay-bronze-demo.md` — replay end-to-end run log.
5. `docs/showcase/slack/airflow-failure-example.json` + `tests/unit/test_callbacks.py`
   — Slack alerting evidence pair (artifact + test).
6. `producers/replay.py` and `jobs/bronze_replay_binance.py` — the only producer/job
   code surfaces touched.
7. `scripts/{bronze,silver,gold,bronze_replay}_count.py` — the AnalysisException fix
   §7.2 covers.
8. `git log --oneline 7c9333a..HEAD` — full session history (16 commits).

To audit by re-running:

```bash
make validate-release   # ~9 min
make reset && make deploy-local && make e2e-local
make replay-bronze-demo && make bronze-replay-count
# Optional (4h):
docker compose --profile live up -d producer-binance
docker compose --profile streaming up -d spark-bronze
make soak-binance
```

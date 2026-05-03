# Plan 1 Closeout — Design Document

**Status:** Approved (2026-05-03)
**Owner:** Fei Huang (autonomous execution by assistant)
**Parent plan:** `docs/superpowers/plans/2026-04-30-financial-lakehouse-mvp.md`
**Estimated effort:** ~1 weekend (5 deliverables; soak runs unattended for 4h)
**Cost:** $0 (no external paid resources)

---

## 1. Goal

Bring the four committed resume bullets to "reviewer-believes-on-first-glance + interviewer-can-click-each-link-to-evidence" quality with zero outside spend, then mark Plan 1 (MVP) Done so Plan 3 (Databricks Free Edition) can begin.

Plan 1 is presently ~95% complete. The local Bronze→Silver→Gold path, Airflow runtime, Delta maintenance, Prometheus metrics, Grafana dashboard, ADRs, and deployable compose stack are all committed. Five gaps remain that prevent the bullets from defending themselves under scrutiny: visual architecture (Task 44), credible CI signals (Tasks 40-42), captured failure-alert evidence (Task 39), an end-to-end replay demonstration through the compose stack (2026-05-02 progress note #2), and live-feed soak evidence to back the "real-time financial market feeds" wording (2026-05-02 progress note #1).

This sub-spec closes those five gaps and nothing else.

## 2. Non-Goals

- **Alpaca soak.** REST polling does not strengthen the "real-time" claim that Binance WS already supports.
- **GHCR image push + docker-compose image references.** Changes the deployment model (compose pull instead of build); deserves its own sub-spec.
- **Plan 2 / 3 / 4 work.** CLAUDE.md gates these; this sub-spec is the close-out before opening Plan 3.
- **Older progress-report cleanup.** Low-value housekeeping; can ride on a separate trivial commit.
- **24h+ soak.** WSL2 host-sleep risk vs. marginal evidence does not justify the schedule risk.
- **Pipeline functional changes.** Bronze/Silver/Gold logic is frozen for this sub-spec; only evidence and packaging change.
- **GHCR push or image versioning.**

## 3. Success Criteria

- README top displays four green GHA status badges (lint-test, dag-validate, integration-test, build-images).
- README embeds the data-flow Mermaid diagram above the fold; `docs/architecture/overview.md` embeds compose-topology and Airflow-dataset diagrams.
- `docs/architecture/diagrams/` contains three `.mmd` source files committed.
- A unit test covers `dags/_common/callbacks.py::alert_on_failure` with full POST-body shape assertions, runs in CI, passes.
- `docs/showcase/slack/` contains: (a) the captured POST body that would be sent (`airflow-failure-example.json`), (b) a one-command README explaining how to capture the human-visible Slack screenshot once a real webhook URL is available.
- `make replay-bronze-demo` runs against a clean MinIO state and produces a row count visible via a `make bronze-replay-count` target. The chain: generate Parquet → ReplayProducer → MinIO landing/replay/binance → Bronze Delta → count.
- `tests/integration/test_replay_compose.py` exercises the same chain on local fs + local Spark and runs in CI.
- `docs/showcase/soak/2026-05-03-binance-soak.md` exists with: start/end timestamps, Bronze record count delta, periodic metrics-publisher snapshots (every ~30 min), producer container log sample showing reconnect resilience (or "no reconnect events observed" with evidence), and one Markdown table summarizing per-symbol record growth. If the soak runs less than the planned 4h due to host-sleep or network issues, the doc reports the actual duration honestly without overclaiming.
- After all changes: `make deploy-local` and `make e2e-local` still succeed.
- All commits land on `main` following existing project convention.

## 4. Per-Deliverable Design

### 4.1 Mermaid Architecture Diagrams

**Files added:**

- `docs/architecture/diagrams/data-flow.mmd` — Producers → MinIO landing → Bronze (Structured Streaming + checkpoint) → Silver (quality split + quarantine) → Gold (aggregations). Includes Replay producer feeding the same landing prefix.
- `docs/architecture/diagrams/compose-topology.mmd` — Service graph: producer-binance, producer-alpaca (live profile), MinIO + minio-init, Spark master + 2 workers, Postgres, Airflow webserver + scheduler, metrics-publisher, Prometheus, Grafana. Edges labelled with protocol/port where meaningful.
- `docs/architecture/diagrams/airflow-datasets.mmd` — DAG dependency via Airflow Datasets: `silver_pipeline` → `gold_aggregations`; `optimize_hot` ↔ `vacuum_nightly` cross-DAG ordering as Plan 1 §J describes.

**Files edited:**

- `README.md` — embed the data-flow diagram inline as a fenced ` ```mermaid ` block immediately after the project description.
- `docs/architecture/overview.md` — embed all three diagrams as inline ` ```mermaid ` blocks, captioned, replacing the bulleted prose summary.

**Validation:**

- Each .mmd file is parsed via `mmdc` (Mermaid CLI) if available; otherwise validated by visual inspection (small diagrams, low risk of malformed syntax).

### 4.2 GHA Workflow Hardening

**Goal:** Four always-green badges visible on the README. The fifth workflow (`benchmark-small`) becomes manual/scheduled because it is not relevant per push.

**Files edited:**

- `.github/workflows/lint-test.yml` — Cache uv via `astral-sh/setup-uv@v5` cache, run ruff check + ruff format --check + mypy + `pytest tests/unit -v`. Triggers on push and pull_request.
- `.github/workflows/dag-validate.yml` — setup-uv with cache, `uv sync --extra dev --extra airflow`, `pytest tests/dag -v`.
- `.github/workflows/integration-test.yml` — setup-java-action@v4 (Temurin 17), setup-uv with cache, `uv sync --all-extras`, `pytest tests/integration -v -m integration`. Concurrency group to cancel superseded runs.
- `.github/workflows/build-images.yml` — `docker/setup-buildx-action@v3` + `docker buildx bake` (build only, no push). Trigger on push to main and pull_request. Add `if: github.event_name == 'pull_request' || github.ref == 'refs/heads/main'`.
- `.github/workflows/benchmark-small.yml` — change trigger to `workflow_dispatch` only (no push trigger). The benchmark is a long-running optional artifact, not a per-push gate.

**README badges (top of file, after H1):**

```markdown
[![lint-test](https://github.com/huangf06/financial-lakehouse/actions/workflows/lint-test.yml/badge.svg)](.../lint-test.yml)
[![dag-validate](.../dag-validate.yml/badge.svg)](.../dag-validate.yml)
[![integration-test](.../integration-test.yml/badge.svg)](.../integration-test.yml)
[![build-images](.../build-images.yml/badge.svg)](.../build-images.yml)
```

**Validation:** Push commit, observe GHA. Iterate fixes (most likely failure modes: missing system Java, missing dependency in extras, slow test timeouts) until four badges go green on `main`.

### 4.3 Slack Failure-Alerting Smoke Test

**Files added:**

- `tests/unit/test_callbacks.py` — Three test cases:
  1. `test_alert_on_failure_posts_expected_body_when_webhook_set` — monkey-patch `urllib.request.urlopen`, set `ALERTS__SLACK_WEBHOOK_URL`, run `alert_on_failure({"task_instance": ...})`, assert request URL, JSON body schema, dag_id/task_id rendered into text.
  2. `test_alert_on_failure_noop_when_webhook_unset` — no env var, no request issued, no exception raised.
  3. `test_alert_on_sla_miss_returns_none` — guard the placeholder so future signature changes do not silently break.
- `scripts/send_test_slack_alert.py` — One-shot CLI that reads `ALERTS__SLACK_WEBHOOK_URL`, constructs the same body the failure callback would send for a synthetic dag/task, posts it, prints HTTP status. Intended for manual one-time evidence capture.
- `docs/showcase/slack/airflow-failure-example.json` — Captured POST body that the callback would emit for a representative failure context. Generated programmatically (not synthesized prose) so the JSON is authoritative.
- `docs/showcase/slack/README.md` — Two-step instructions: (1) export `ALERTS__SLACK_WEBHOOK_URL`, (2) run `python scripts/send_test_slack_alert.py`, then take a screenshot of the resulting Slack message and save as `airflow-failure-2026-05-XX.png` in the same folder. Documents that the JSON body is the machine-verifiable artifact and the screenshot is the human-readable companion.

**Validation:**

- `pytest tests/unit/test_callbacks.py -v` passes.
- `cat docs/showcase/slack/airflow-failure-example.json` shows a valid JSON body matching the runtime callback output.

### 4.4 Compose-Level Historical Replay End-to-End Demo

**Files added:**

- `scripts/seed_replay_parquet.py` — Generates a deterministic ~50-row Parquet at `data/replay/binance_2024-01-01.parquet` (gitignored). Schema mirrors the Binance trade payload that the existing replay producer reads. Seed is fixed so output is reproducible across runs.
- `scripts/bronze_replay_count.py` — Reads the Bronze Delta table at the replay-source path and prints record count. Mirror of `scripts/bronze_count.py` but for the replay landing prefix.
- `tests/integration/test_replay_compose.py` — End-to-end test on local fs (no MinIO required, mirrors existing integration-test pattern):
  1. Generate Parquet via `scripts/seed_replay_parquet.py` to a temp dir.
  2. Run `ReplayProducer` against a temp landing dir.
  3. Run Bronze stream reader (`pipelines.bronze.stream_reader`) against the JSONL output, write to a temp Delta path with checkpoint.
  4. Assert Delta count matches the Parquet input row count.
  5. Re-run Bronze; assert idempotency (no duplicate rows due to checkpoint).

**Files edited:**

- `pipelines/config/settings.py` — Add a `Settings.landing_path` overload or accept arbitrary source paths; current implementation supports any string source already (`landing/{source}`), so this likely needs no change beyond verifying `replay/binance` works as a source key.
- `Makefile` — New targets:
  - `seed-replay`: invoke `scripts/seed_replay_parquet.py`.
  - `replay-bronze-demo`: full chain — seed-replay → run replay producer to MinIO `landing/replay/binance/` → bronze-once for source `replay/binance` → count.
  - `bronze-replay-count`: invoke `scripts/bronze_replay_count.py`.
- `.gitignore` — Ensure `data/replay/` is ignored.

**Validation:**

- `make seed-replay` produces the Parquet locally.
- `make replay-bronze-demo` against a clean compose stack lands ≥50 rows in the Bronze replay-binance Delta table.
- `pytest tests/integration/test_replay_compose.py -v -m integration` passes locally and in CI.

### 4.5 4-Hour Binance Soak

**Procedure (executed by the assistant, runs unattended):**

1. **Pre-flight:** `make reset && make deploy-local`. Verify metrics-publisher and producer-binance are healthy (`docker compose ps`, `make metrics-snapshot`).
2. **Start soak:** record start timestamp; `docker compose up -d producer-binance` (already up after `deploy-local`); start Bronze streaming (`docker compose --profile streaming up -d spark-bronze`).
3. **Periodic capture (every 30 min, 8 snapshots over 4h):**
   - `make bronze-count` → record into evidence file.
   - `make metrics-snapshot` → record into evidence file.
   - `docker compose logs --tail 50 producer-binance` → record into evidence file.
4. **End:** record end timestamp; `docker compose --profile streaming down`.
5. **Aggregate:** consolidate all snapshots into `docs/showcase/soak/2026-05-03-binance-soak.md` with summary table (timestamp, total rows, delta, per-symbol counts, reconnect markers if any).

**Files added:**

- `scripts/run_binance_soak.sh` — Idempotent bash wrapper that runs the procedure above, writing snapshots to `docs/showcase/soak/_raw/`. Exits gracefully on SIGINT so a partial soak still leaves usable evidence.
- `docs/showcase/soak/2026-05-03-binance-soak.md` — Final consolidated evidence doc (assistant writes after the soak completes or after partial-soak interruption).

**Substitution for missing Grafana screenshot:**

The plan agreed to capture Grafana panel screenshots. The assistant cannot operate a browser. Substitute with:

- A markdown table of metrics-publisher snapshots over time (timestamp, gauge name, value).
- An ASCII chart of Bronze record growth over the soak window.
- A second-pass instruction in `docs/showcase/soak/README.md` for the user to take a Grafana screenshot manually if they want the visual artifact.

This substitution is honest: the README will state what was captured by automation and what is left as manual follow-up.

**Honest-fallback rule:** if the soak terminates early (host sleep, network, container crash), the evidence doc reports the actual duration without rebranding "4h soak" to "8m soak". Bullet 1 wording still defensible at the partial duration as long as the captured evidence is genuine.

## 5. Sequencing and Effort Estimate

Wall-clock dependencies, not engineering hours:

1. **Phase A (sequential, ~30 min total):** Diagrams (4.1) → README badge wiring (4.2 partial — files only, badge URLs static) → Slack test + script + example body (4.3) → seed-replay + bronze-replay-count + replay test (4.4 partial).
2. **Phase B (sequential, ~60-90 min, requires green CI):** Push Phase A commits, observe GHA workflows, iterate fixes until four badges green.
3. **Phase C (sequential, ~15 min):** Run `make replay-bronze-demo` against compose stack to verify end-to-end (4.4 final).
4. **Phase D (unattended, ~4 hours wall-clock):** Run 4h Binance soak (4.5). Assistant uses `ScheduleWakeup` to check in periodically and capture snapshots. Soak runs in background; assistant continues no other code work during this phase.
5. **Phase E (~30 min):** Aggregate soak evidence doc, final commit, update CLAUDE.md status block, mark Plan 1 Done.

Total assistant attention: ~3 hours active + 4h passive soak monitoring.

## 6. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| GHA integration tests reveal hidden Spark/Java setup issues that need iteration | Medium | Adds 1-2 hours to Phase B | Iterate honestly; if integration cannot go green in a reasonable budget, downgrade `integration-test.yml` to a smaller subset and document the constraint |
| WSL2 host sleeps during soak, terminating producer | Medium | Soak ends early | Assistant honest-fallback rule above; document partial-duration evidence rather than overclaiming |
| Mermaid syntax invalid on first write | Low | 5-min fix | Validate via local mermaid CLI or by pushing and viewing GitHub render |
| `docker compose --profile streaming up -d spark-bronze` interferes with running compose stack | Low | Could stall soak | Check via `docker compose ps` before starting; defer to manual restart if needed |
| User has no Slack workspace to capture screenshot | High | Visual evidence absent | Substitution covered in §4.3 — POST body JSON is the machine-verifiable artifact, screenshot is documented as deferred manual step |
| Grafana screenshot impossible without browser | High | Visual evidence absent | Substitution covered in §4.5 — text-table metrics over time is the honest substitute |
| Replay-source path collides with existing Bronze configuration | Low | Test fails | Source key `replay/binance` is path-isolated from existing `binance` source; bronze can be invoked with explicit landing-path override |
| 4h continuous Binance WS hits rate limit or IP throttle | Low | Producer reconnects logged, evidence remains valid (proves reconnect resilience) | Treat reconnect markers as positive evidence rather than failure |

## 7. Files Touched Summary

**New:**

- `docs/architecture/diagrams/data-flow.mmd`
- `docs/architecture/diagrams/compose-topology.mmd`
- `docs/architecture/diagrams/airflow-datasets.mmd`
- `tests/unit/test_callbacks.py`
- `tests/integration/test_replay_compose.py`
- `scripts/seed_replay_parquet.py`
- `scripts/bronze_replay_count.py`
- `scripts/send_test_slack_alert.py`
- `scripts/run_binance_soak.sh`
- `docs/showcase/slack/README.md`
- `docs/showcase/slack/airflow-failure-example.json`
- `docs/showcase/soak/README.md`
- `docs/showcase/soak/2026-05-03-binance-soak.md`

**Edited:**

- `README.md` — top badges, embedded data-flow diagram
- `docs/architecture/overview.md` — embedded all three diagrams
- `.github/workflows/lint-test.yml`
- `.github/workflows/dag-validate.yml`
- `.github/workflows/integration-test.yml`
- `.github/workflows/build-images.yml`
- `.github/workflows/benchmark-small.yml`
- `Makefile` — new targets seed-replay, replay-bronze-demo, bronze-replay-count, soak-binance
- `.gitignore` — ignore `data/replay/`
- `CLAUDE.md` — bump status block once Plan 1 Done

## 8. Acceptance Test (one command after all phases)

```bash
make validate-release && \
make deploy-local && make e2e-local && \
make replay-bronze-demo && make bronze-replay-count && \
ls docs/architecture/diagrams/*.mmd && \
ls docs/showcase/slack/airflow-failure-example.json && \
ls docs/showcase/soak/2026-05-03-binance-soak.md
```

All four GHA workflow badges (lint-test, dag-validate, integration-test, build-images) green on `main` in addition to the local checks above.

## 9. Open Questions Carried to Future Work

- Should `make e2e-local` integrate the replay-bronze-demo as a final step? (Current sub-spec keeps them separate to avoid slowing the existing E2E.)
- Should the soak be reproduced on Plan 4 (Oracle Cloud) infrastructure once that exists, to demonstrate that the same soak procedure runs on a $0 always-on substrate? (Planned in Plan 4, not here.)
- Should image versioning + GHCR push be a Plan-1.5 sub-spec or rolled into Plan 2? (Decision deferred.)

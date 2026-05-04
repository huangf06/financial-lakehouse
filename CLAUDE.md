# Financial Data Lakehouse — Agent Bootstrap

Portfolio data engineering project at `/home/huang/financial-lakehouse/`. Design and implementation plan are committed, and the local MVP now has runnable Bronze, Silver, Gold, Airflow, benchmark, and metrics evidence. New sessions should pick up by reading the current progress notes and continuing from the remaining Plan 1 gaps.

## Authoritative documents (read before doing anything)

- **Design spec**: `docs/superpowers/specs/2026-04-30-financial-lakehouse-design.md` (655 lines — the WHAT and WHY)
- **Implementation plan (MVP)**: `docs/superpowers/plans/2026-04-30-financial-lakehouse-mvp.md` (7796 lines, 45 tasks, full code per step)

Do not redesign or regenerate these without explicit approval. They were brainstormed across 9 design sections and approved by the user on 2026-04-30.

## Resume bullets being backed (current deployable-stack wording)

The four bullets the current repository can defend without AWS/Databricks deployment evidence:

1. *Built a deployable financial data lakehouse with Spark Structured Streaming and Delta Lake, landing market-style JSONL feeds in MinIO/S3A and ingesting them into checkpointed Bronze tables with integration tests covering additive schema tolerance and restart recovery.*
2. *Implemented a declarative data quality framework for Bronze-to-Silver validation across trades and bars, routing malformed records into quarantine tables and demonstrating rule-based replay that recovers corrected records into Silver without hand-editing data.*
3. *Added Delta Lake maintenance jobs for compaction, Z-order optimization on symbol, and vacuum; benchmarked 100k-row analytical queries to compare baseline, compacted, and Z-ordered table layouts for time-series access patterns.*
4. *Packaged the lakehouse as a Docker Compose deployment with Airflow DAGs, shared failure callbacks, Prometheus/Grafana observability, deployment checks, and an end-to-end Bronze-to-Gold validation command.*

Do not claim `Databricks/Spark/AWS`, `ensuring zero data loss`, or production readiness until the
deferred deployment and live-feed evidence exists. See
`docs/resume/bullet-library-review.md` for the external bullet-library recommendation and the
alternate quality bullet to use if replay is removed.

## Plan inventory (DO NOT pre-write Plans 2–4)

| Plan | Status | When to write |
|---|---|---|
| Plan 1 — MVP Lakehouse Engine (Tasks 1–45) | ✅ Written | — |
| Plan 2 — AWS Showcase deployment | ⏳ Deferred | Write when Plan 1 is ~80% complete |
| Plan 3 — Databricks Asset Bundle | ⏳ Deferred | Write after Plan 2 |
| Plan 4 — Personal Frugal (Oracle Cloud) | ⏳ Deferred | Write last |

Each plan covers 0.5–1.5 weekends. Writing them prematurely creates stale plans (tooling versions move).

## Deployment profile intents

| Profile | Lifecycle | DO NOT |
|---|---|---|
| `compose` (default, local) | Persistent dev | — |
| `aws-showcase` | **Ephemeral** — `make aws-up` → demo → `make aws-down` | Configure as always-on; daily cost > $50 |
| `personal-frugal` (Oracle Cloud Free Tier) | Always-on, $0/month | Use AWS for this — Oracle is the chosen long-term substrate |
| `databricks` (Free Edition) | On-demand sessions | Configure as production |

## Tech pins (do not silently upgrade)

- Python 3.11 (not 3.12 — PySpark 3.5 stability)
- PySpark 3.5.3
- Delta Lake 3.2.0 (matches PySpark 3.5)
- Airflow 2.10.3 (Datasets feature stable)
- Java 17 (required by Spark 3.5)
- MinIO RELEASE.2024-10+
- uv 0.4.x

## Out of scope (do not add unprompted)

- ML on Gold layer (separate future portfolio project)
- L1 quotes / bid-ask spread analysis (roadmap mention only)
- Cross-source dedup / arbitrage analysis
- Coinbase data source (deliberately dropped during design — Binance suffices)
- 15m/30m/4h bar timeframes (only 1m/5m/1h/1d are built)
- Always-on Databricks deployment

## Execution mode

This project is for **deep portfolio learning**, not pure code generation. See `feedback_portfolio_learning_intent.md` in memory for guidance. Default pattern: subagent-driven-development with explicit per-task review gates and explanation-on-demand. Avoid silent batch execution.

## Conventions

- All runtime config via env vars validated by `pydantic-settings` (`pipelines/config/settings.py`). **Never hardcode** paths, endpoints, or credentials.
- One profile = one `config/profiles/*.env` file.
- Docker images pre-built via `docker-bake.hcl`, pushed to GHCR. Users `docker compose pull` rather than rebuilding.
- Tests tiered: unit (<30s) / integration (<5min) / DAG static (<10s).
- Resume claims anchored in README's Evidence Map (§Resume Claims → Evidence). Every claim points to a code path or test.

## Status as of 2026-05-04

- ✅ Plan 1 (MVP, Tasks 1-45) **Done** after the closeout sub-spec landed:
  - Mermaid architecture diagrams committed under `docs/architecture/diagrams/` and
    embedded in `README.md` and `docs/architecture/overview.md` (closes Task 44).
  - Four GHA workflow badges (lint-test, dag-validate, integration-test, build-images)
    green on `main`; benchmark-small dropped from per-push trigger to manual dispatch
    (closes Tasks 40-42).
  - Slack failure-callback unit test in CI (`tests/unit/test_callbacks.py`); captured
    POST body in `docs/showcase/slack/airflow-failure-example.json`; visual screenshot
    capture documented in `docs/showcase/slack/README.md` (closes Task 39).
  - Compose-level historical replay end-to-end demo wired through `make
    replay-bronze-demo` (replay Parquet → MinIO → Bronze Delta) with
    `tests/integration/test_replay_compose.py` covering the chain on local fs;
    captured run evidence in `docs/showcase/replay/`.
  - 4-hour Binance public-WS soak with continuous Bronze ingest captured in
    `docs/showcase/soak/2026-05-03-binance-soak.md` (+431,106 records over 3h 54min,
    monotonic Bronze growth, no producer/stream restarts observed).
- ✅ Sub-spec design and plan: `docs/superpowers/specs/2026-05-03-plan1-closeout-design.md`
  and `docs/superpowers/plans/2026-05-03-plan1-closeout.md`.
- ✅ Resume bullet 1 wording can drop the "no live soak" caveat — soak evidence exists.
- ⏳ Deferred: AWS showcase (Plan 2), Databricks Asset Bundle deployment (Plan 3),
  Oracle personal-frugal deployment (Plan 4), GHCR image push + docker compose pull
  switch (own sub-spec).

A new session should start by reading `CLAUDE.md`, `docs/showcase/soak/2026-05-03-binance-soak.md`,
`docs/superpowers/specs/2026-05-03-plan1-closeout-design.md`,
`docs/resume/bullet-library-review.md`, and the current git log. Plan 1 is done; the
recommended next step is to start brainstorming Plan 3 (Databricks Asset Bundle, Free
Edition) since it is $0 and restores the `Databricks` keyword in the resume title.

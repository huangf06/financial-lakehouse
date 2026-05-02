# Financial Data Lakehouse — Agent Bootstrap

Portfolio data engineering project at `/home/huang/financial-lakehouse/`. Design and implementation plan are committed, and the local MVP now has runnable Bronze, Silver, Gold, Airflow, benchmark, and metrics evidence. New sessions should pick up by reading the current progress notes and continuing from the remaining Plan 1 gaps.

## Authoritative documents (read before doing anything)

- **Design spec**: `docs/superpowers/specs/2026-04-30-financial-lakehouse-design.md` (655 lines — the WHAT and WHY)
- **Implementation plan (MVP)**: `docs/superpowers/plans/2026-04-30-financial-lakehouse-mvp.md` (7796 lines, 45 tasks, full code per step)

Do not redesign or regenerate these without explicit approval. They were brainstormed across 9 design sections and approved by the user on 2026-04-30.

## Resume bullets being backed (current local-MVP wording)

The four bullets the current repository can defend without AWS/Databricks deployment evidence:

1. *Built a local financial data lakehouse with Spark Structured Streaming and Delta Lake, ingesting market-style JSONL feeds from MinIO into Bronze tables with persisted checkpoints and integration tests covering additive schema tolerance and restart recovery.*
2. *Implemented a declarative data quality framework for Bronze-to-Silver validation, routing malformed records into quarantine tables and demonstrating rule-based replay that recovers corrected records into Silver without hand-editing data.*
3. *Added Delta Lake maintenance jobs for compaction, Z-order optimization on symbol, and vacuum; benchmarked 100k-row analytical queries to compare baseline, compacted, and Z-ordered table layouts.*
4. *Orchestrated Silver, Gold, replay, optimization, and vacuum jobs with Airflow DAGs, shared failure callbacks, and Docker Compose services for reproducible local execution and review.*

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

## Status as of 2026-05-02

- ✅ Design committed (commit `e3f9d98`)
- ✅ Plan 1 (MVP) committed (commit `6a53a39`)
- ✅ Phase 1 local foundation, compose stack, Bronze/Silver/Gold runtime loops, Airflow compose runtime, local benchmark, and metrics publisher are implemented and committed.
- ✅ Latest local status is recorded in `docs/progress/2026-05-02-project-status.md`.
- ✅ Deployable local stack work is recorded in `docs/progress/2026-05-03-deployable-stack.md`.
- ✅ Resume bullet wording review is recorded in `docs/resume/bullet-library-review.md`.
- ✅ Local deployment entrypoints exist: `make deploy-local`, `make e2e-local`, and `make deploy-live`.
- ⏳ Deferred: AWS showcase, Databricks Asset Bundle deployment, Oracle personal-frugal deployment, and long-running live external producer soak validation.

A new session should start by reading `CLAUDE.md`, `docs/progress/2026-05-02-project-status.md`,
`docs/progress/2026-05-03-deployable-stack.md`, `docs/resume/bullet-library-review.md`, and the
current git log before choosing the next remaining Plan 1 gap.

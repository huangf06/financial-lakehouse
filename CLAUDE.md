# Financial Data Lakehouse — Agent Bootstrap

Portfolio data engineering project at `/home/huang/financial-lakehouse/`. Design and implementation plan are committed, and the local MVP now has runnable Bronze, Silver, Gold, Airflow, benchmark, and metrics evidence. New sessions should pick up by reading the current progress notes and continuing from the remaining Plan 1 gaps.

## Authoritative documents (read before doing anything)

- **Design spec**: `docs/superpowers/specs/2026-04-30-financial-lakehouse-design.md` (655 lines — the WHAT and WHY)
- **Implementation plan (MVP)**: `docs/superpowers/plans/2026-04-30-financial-lakehouse-mvp.md` (7796 lines, 45 tasks, full code per step)

Do not redesign or regenerate these without explicit approval. They were brainstormed across 9 design sections and approved by the user on 2026-04-30.

## Resume bullets being backed (REVISED wording — do not revert)

The four bullets the project must defend, with deliberate revisions documented in spec §1:

1. *Architected lakehouse on Databricks/Spark processing real-time market feeds via Auto Loader and Structured Streaming; **configured** schema evolution and checkpoint-based fault tolerance, **validated zero data loss with integration tests**.*
2. *Engineered data quality framework with quarantine-and-replay pattern across Bronze/Silver/Gold; achieved automated recovery without manual intervention.*
3. *Optimized Delta Lake performance through Z-ordering on high-cardinality columns and automated compaction; designed partition strategy aligned with time-series access patterns.*
4. *Integrated **Airflow (OSS deployment) and Databricks Jobs (managed deployment)** for dependency-aware orchestration with failure alerting; containerized via Docker for reproducible deployment.*

**Why the wording was softened (do not revert):**
- Bullet 1: "implemented schema evolution" → "configured + validated". Schema evolution is a platform feature (Auto Loader / Delta `mergeSchema`), not custom-built. The honest claim is configuration + integration test proof.
- Bullet 4: expanded to name Databricks Jobs explicitly because the Databricks profile uses Jobs (not Airflow on DBX) for orchestration. This is platform-appropriate dual-orchestration, not "replacing Airflow".

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

## Status as of 2026-05-01

- ✅ Design committed (commit `e3f9d98`)
- ✅ Plan 1 (MVP) committed (commit `6a53a39`)
- ✅ Phase 1 local foundation, compose stack, Bronze/Silver/Gold runtime loops, Airflow compose runtime, local benchmark, and metrics publisher are implemented and committed.
- ✅ Latest validation is recorded in `docs/progress/2026-05-01-codex-execution-notes.md`.
- ⏳ Deferred: AWS showcase, Databricks Asset Bundle deployment, Oracle personal-frugal deployment, and live external producer validation.

A new session should start by reading `CLAUDE.md`, `docs/progress/2026-05-01-codex-execution-notes.md`, and the current git log before choosing the next remaining Plan 1 gap.

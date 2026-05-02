# Financial Lakehouse Resume Bullet Review

Date: 2026-05-02

## Verdict

The project is strong enough for a portfolio demo and data-engineering resume section, but the
current bullet-library entry overclaims in three places:

- `Databricks/Spark/AWS` in the title is ahead of the implemented evidence. Current evidence is
  local Spark, Delta Lake, MinIO, Airflow, Prometheus, and Grafana. AWS and Databricks deployments
  are deferred.
- `ensuring zero data loss` is too absolute. The defensible claim is checkpoint-based recovery and
  no duplicate reprocessing in the local integration path.
- `automated recovery without manual intervention` depends on keeping replay. If replay is removed,
  the quality bullet should only claim quarantine, observability, and deterministic reprocessing
  hooks.

The submitted period `Oct. 2025 - Jan. 2026` does not match this repository's evidence trail, which
is dated April-May 2026. If the external resume library is meant to reflect this repo, use a period
aligned with the actual work, for example `Apr. 2026 - May 2026`.

## Recommended Library Entry

This version matches the current repository if replay remains in scope:

```yaml
financial_data_lakehouse:
  title: "Financial Data Lakehouse (Spark/Delta/Airflow)"
  institution: "Personal Portfolio Project"
  period: "Apr. 2026 - May 2026"
  recommended_sequences:
    data_engineer: ["lakehouse_streaming", "lakehouse_quality", "lakehouse_optimization", "lakehouse_orchestration"]
  verified_bullets:
    - id: lakehouse_streaming
      status: active
      narrative_role: optional
      content: "Built a local financial data lakehouse with Spark Structured Streaming and Delta Lake, ingesting market-style JSONL feeds from MinIO into Bronze tables with persisted checkpoints and integration tests covering additive schema tolerance and restart recovery."
    - id: lakehouse_quality
      status: active
      narrative_role: optional
      content: "Implemented a declarative data quality framework for Bronze-to-Silver validation, routing malformed records into quarantine tables and demonstrating rule-based replay that recovers corrected records into Silver without hand-editing data."
    - id: lakehouse_optimization
      status: active
      narrative_role: optional
      content: "Added Delta Lake maintenance jobs for compaction, Z-order optimization on symbol, and vacuum; benchmarked 100k-row analytical queries to compare baseline, compacted, and Z-ordered table layouts."
    - id: lakehouse_orchestration
      status: active
      narrative_role: optional
      content: "Orchestrated Silver, Gold, replay, optimization, and vacuum jobs with Airflow DAGs, shared failure callbacks, and Docker Compose services for reproducible local execution and review."
```

## If Replay Is Removed

Use this quality bullet instead:

```yaml
    - id: lakehouse_quality
      status: active
      narrative_role: optional
      content: "Implemented a declarative data quality framework for Bronze-to-Silver validation, routing malformed records into quarantine tables with rule-level failure metadata for investigation and deterministic reprocessing."
```

Also update the recommended sequence to remove replay-specific emphasis from project discussion:

```yaml
recommended_sequences:
  data_engineer: ["lakehouse_streaming", "lakehouse_quality", "lakehouse_optimization", "lakehouse_orchestration"]
```

The sequence can stay the same because `lakehouse_quality` still exists; only its claim changes.

## Future Upgrade Path

Use stronger wording only after new evidence lands:

- Add `Databricks` back into the title after a Databricks Asset Bundle or notebook/job run is
  committed with screenshots or logs.
- Add `AWS` back into the title after the AWS showcase profile exists and has teardown-safe
  deployment evidence.
- Use `real-time financial market feeds` after the Binance/Alpaca producers have a documented
  compose run against external APIs.
- Use `zero data loss` only if there is a bounded failure matrix covering producer disconnects,
  landing writes, Spark restarts, checkpoint recovery, and duplicate prevention.

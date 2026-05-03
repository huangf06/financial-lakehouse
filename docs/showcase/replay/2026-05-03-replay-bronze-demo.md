# Compose-Level Replay-to-Bronze Demo Run

Date: 2026-05-03T14:57:52Z
Commit: 89da8cb

## Procedure

```bash
make reset
make deploy-local
make replay-bronze-demo
make bronze-replay-count
```

## Result

`scripts/seed_replay_parquet.py` generated 50 deterministic Binance-shaped trades
into `data/replay/binance_2024-01-01.parquet`. The host-side `ReplayProducer` then
streamed those records as JSONL into `s3a://lakehouse/landing/replay/binance/...` via
`S3JsonlWriter`. The compose Spark master executed `bronze_replay_binance.py`, which
read the JSONL with `BINANCE_TRADE_SCHEMA` and wrote a checkpointed Bronze Delta
table at `s3a://lakehouse/bronze/binance_replay_trades`.

### Spark structured streaming progress (excerpt)

```
"sources" : [ {
  "description" : "FileStreamSource[s3a://lakehouse/landing/replay/binance]",
  "numInputRows" : 50,
  "processedRowsPerSecond" : 3.24
} ],
"sink" : {
  "description" : "DeltaSink[s3a://lakehouse/bronze/binance_replay_trades]"
}
```

### Final count

```
=== BRONZE REPLAY COUNT: 50 records ===
```

## Why This Matters

This is the first end-to-end evidence that the existing replay producer (originally
a local-fs-only Parquet -> JSONL utility) survives the compose orchestration path:
host -> MinIO landing prefix -> Spark structured streaming -> Bronze Delta -> count.
Bullet 2 (`automated recovery`) had unit-test evidence and a quarantine-replay demo;
this adds the missing chain that proves the historical replay path is wired into the
same medallion that production data flows through.

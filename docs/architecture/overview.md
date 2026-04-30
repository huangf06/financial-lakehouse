# Architecture Overview

The local MVP uses a medallion lakehouse:

1. Producers write JSONL files to `landing/` using `.tmp` files followed by atomic rename.
2. Bronze Structured Streaming reads complete files and writes Delta with checkpoint recovery.
3. Silver normalizes trades and bars, then splits invalid records into quarantine tables.
4. Gold produces bars, daily volume profile, and market-quality metrics.
5. Airflow coordinates Silver, Gold, replay, optimize, and vacuum jobs.

See the Evidence Map in `README.md` for the code paths backing each resume claim.

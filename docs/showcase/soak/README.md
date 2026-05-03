# Live Producer Soak Evidence

The soak proves bullet 1's `real-time financial market feeds` claim: the producer-binance
container, configured against the public Binance WS, drives a sustained Bronze record growth
under continuous live conditions while the metrics-publisher reports gauges from the actual
Delta transaction logs.

## How To Run

```bash
make reset
make deploy-local
docker compose --profile streaming up -d spark-bronze
make soak-binance
```

`make soak-binance` runs `scripts/run_binance_soak.sh` for `SOAK_DURATION_SEC` (default
14400 = 4h), capturing one snapshot every `SOAK_INTERVAL_SEC` (default 1800 = 30min). Each
snapshot writes to `docs/showcase/soak/_raw/snapshot-<n>-<ts>.txt` and contains:

- the current `make bronze-count`,
- a `make metrics-snapshot` (one Prometheus dump from `metrics-publisher`),
- the last 80 lines of `docker compose logs producer-binance`.

`_raw/start.txt` and `_raw/end.txt` capture the bracketing timestamps. The raw directory is
gitignored; the consolidated final doc (`2026-05-XX-binance-soak.md`) lives next to this
README and is what gets committed.

## How To Interpret The Evidence

Bullet 1 needs three things to hold:

1. Continuous record growth across snapshots.
2. No silent producer crash; if the container restarted, the log tail must show the reconnect
   and the next snapshot's count must continue increasing.
3. Bronze checkpoint stayed valid; the same row count never goes backward.

If any of those break, the consolidated doc must say so honestly. Partial-duration soaks are
labelled with their actual duration and not rebranded.

## Honest Substitutions

The original plan called for Grafana panel screenshots. The assistant cannot operate a
browser, so the substitutes baked into the procedure are:

- per-snapshot text dump of `make metrics-snapshot` (the same numbers Grafana reads from
  Prometheus),
- per-snapshot Bronze record count (the headline rate-of-progress signal),
- per-snapshot producer log tail (proves the container survived).

A human can take a Grafana screenshot at any point during the soak by opening
`http://localhost:3000` and saving as `grafana-<panel>-YYYY-MM-DD.png` here. That step is
optional; the text-based evidence is the resume-defensible artifact.

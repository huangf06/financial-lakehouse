#!/usr/bin/env bash
# 4-hour Binance public WS soak with periodic snapshots.
#
# Captures every 30 minutes:
#   - Bronze record count
#   - metrics-publisher snapshot
#   - producer-binance log tail
# Writes raw snapshots to docs/showcase/soak/_raw/.
# Idempotent: safe to re-run; raw dir is recreated each run.
set -euo pipefail

DURATION_SEC="${SOAK_DURATION_SEC:-14400}"      # default 4h
INTERVAL_SEC="${SOAK_INTERVAL_SEC:-1800}"       # default 30min
RAW_DIR="docs/showcase/soak/_raw"

mkdir -p "$RAW_DIR"
START_TS="$(date -u +%Y%m%dT%H%M%SZ)"
echo "$START_TS" > "$RAW_DIR/start.txt"

snapshot() {
    local ts="$1"
    local n="$2"
    {
        echo "=== snapshot $n at $ts ==="
        echo "--- metrics snapshot ---"
        # metrics-publisher reads Delta transaction logs directly via boto3 (no Spark
        # cluster contention with the running spark-bronze stream). Bronze record count
        # is captured as the lakehouse_bronze_binance_records_total gauge.
        make metrics-snapshot 2>&1 || echo "(metrics-snapshot failed)"
        echo "--- producer log tail ---"
        docker compose logs --tail 80 producer-binance 2>&1 || echo "(log tail failed)"
        echo "--- spark-bronze log tail ---"
        docker compose logs --tail 40 spark-bronze 2>&1 | tail -40 || echo "(log tail failed)"
    } > "$RAW_DIR/snapshot-${n}-${ts}.txt"
}

ELAPSED=0
N=0
snapshot "$START_TS" "$N"
while [ "$ELAPSED" -lt "$DURATION_SEC" ]; do
    if [ $((DURATION_SEC - ELAPSED)) -lt "$INTERVAL_SEC" ]; then
        sleep_for=$((DURATION_SEC - ELAPSED))
    else
        sleep_for="$INTERVAL_SEC"
    fi
    sleep "$sleep_for"
    ELAPSED=$((ELAPSED + sleep_for))
    N=$((N + 1))
    TS="$(date -u +%Y%m%dT%H%M%SZ)"
    snapshot "$TS" "$N"
done

END_TS="$(date -u +%Y%m%dT%H%M%SZ)"
echo "$END_TS" > "$RAW_DIR/end.txt"
echo "soak complete: $START_TS -> $END_TS, $((N+1)) snapshots in $RAW_DIR"

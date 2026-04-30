"""Metric query definitions."""

from __future__ import annotations

METRIC_QUERIES = {
    "lakehouse_quarantine_records_total": "SELECT count(*) AS value FROM silver.quarantine_trades",
    "lakehouse_gold_bars_total": "SELECT count(*) AS value FROM gold.bars_1m",
}

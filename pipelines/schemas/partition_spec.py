"""Single source of truth for partition and Z-order columns."""

from __future__ import annotations

_PARTITION_COLUMNS: dict[tuple[str, str], list[str]] = {
    ("bronze", "binance_trades"): ["ingestion_date"],
    ("bronze", "alpaca_bars"): ["ingestion_date"],
    ("bronze", "yfinance_history"): ["ingestion_date"],
    ("silver", "trades"): ["event_date"],
    ("silver", "bars"): ["event_date"],
    ("silver", "quarantine_trades"): ["quarantined_date"],
    ("silver", "quarantine_bars"): ["quarantined_date"],
    ("gold", "bars_1m"): ["bar_date"],
    ("gold", "bars_5m"): ["bar_date"],
    ("gold", "bars_1h"): ["bar_date"],
    ("gold", "bars_1d"): ["bar_date"],
    ("gold", "daily_volume_profile"): ["bar_date"],
    ("gold", "market_quality"): ["metric_hour"],
    ("gold", "quarantine_bars"): ["quarantined_date"],
    ("gold", "maintenance_metrics"): ["run_date"],
}

_ZORDER_COLUMNS: dict[tuple[str, str], list[str]] = {
    ("silver", "trades"): ["symbol"],
    ("silver", "bars"): ["symbol"],
    ("gold", "bars_1m"): ["symbol"],
    ("gold", "bars_5m"): ["symbol"],
    ("gold", "bars_1h"): ["symbol"],
    ("gold", "bars_1d"): ["symbol"],
    ("gold", "daily_volume_profile"): ["symbol"],
    ("gold", "market_quality"): ["symbol"],
}


def partition_columns(layer: str, table: str) -> list[str]:
    return _PARTITION_COLUMNS[(layer, table)]


def zorder_columns(layer: str, table: str) -> list[str]:
    return _ZORDER_COLUMNS.get((layer, table), [])

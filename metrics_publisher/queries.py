"""Metric table definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaTableMetric:
    metric_name: str
    layer: str
    table: str
    help_text: str


DELTA_TABLE_METRICS = [
    DeltaTableMetric(
        metric_name="lakehouse_bronze_binance_records_total",
        layer="bronze",
        table="binance_trades",
        help_text="Active records in the Bronze Binance Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_bronze_alpaca_bars_records_total",
        layer="bronze",
        table="alpaca_bars",
        help_text="Active records in the Bronze Alpaca bars Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_silver_trades_records_total",
        layer="silver",
        table="trades",
        help_text="Active records in the Silver trades Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_silver_quarantine_records_total",
        layer="silver",
        table="quarantine_trades",
        help_text="Active records in the Silver trade quarantine Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_silver_bars_records_total",
        layer="silver",
        table="bars",
        help_text="Active records in the Silver bars Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_silver_bar_quarantine_records_total",
        layer="silver",
        table="quarantine_bars",
        help_text="Active records in the Silver bar quarantine Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_gold_daily_volume_records_total",
        layer="gold",
        table="daily_volume_profile",
        help_text="Active records in the Gold daily volume profile Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_gold_market_quality_records_total",
        layer="gold",
        table="market_quality",
        help_text="Active records in the Gold market quality Delta table",
    ),
    DeltaTableMetric(
        metric_name="lakehouse_gold_bars_5m_records_total",
        layer="gold",
        table="bars_5m",
        help_text="Active records in the Gold 5m bars Delta table",
    ),
]

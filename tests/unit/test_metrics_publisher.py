"""Metrics publisher unit tests."""

from __future__ import annotations

import json

from metrics_publisher.publisher import _parse_s3a_path, _record_count_from_delta_actions
from metrics_publisher.queries import DELTA_TABLE_METRICS


def test_parse_s3a_path() -> None:
    parsed = _parse_s3a_path("s3a://lakehouse/silver/trades")

    assert parsed.bucket == "lakehouse"
    assert parsed.key == "silver/trades"


def test_delta_action_record_count_tracks_active_files() -> None:
    lines = [
        json.dumps({"add": {"path": "a.parquet", "stats": json.dumps({"numRecords": 10})}}),
        json.dumps({"add": {"path": "b.parquet", "stats": json.dumps({"numRecords": 5})}}),
        json.dumps({"remove": {"path": "a.parquet"}}),
        json.dumps({"add": {"path": "c.parquet", "stats": json.dumps({"numRecords": 7})}}),
    ]

    active = _record_count_from_delta_actions(lines)

    assert active == {"b.parquet": 5, "c.parquet": 7}
    assert sum(active.values()) == 12


def test_delta_table_metrics_cover_trade_and_bar_layers() -> None:
    metric_tables = {
        (metric.metric_name, metric.layer, metric.table) for metric in DELTA_TABLE_METRICS
    }

    assert metric_tables == {
        ("lakehouse_bronze_binance_records_total", "bronze", "binance_trades"),
        ("lakehouse_bronze_alpaca_bars_records_total", "bronze", "alpaca_bars"),
        ("lakehouse_silver_trades_records_total", "silver", "trades"),
        ("lakehouse_silver_quarantine_records_total", "silver", "quarantine_trades"),
        ("lakehouse_silver_bars_records_total", "silver", "bars"),
        ("lakehouse_silver_bar_quarantine_records_total", "silver", "quarantine_bars"),
        ("lakehouse_gold_daily_volume_records_total", "gold", "daily_volume_profile"),
        ("lakehouse_gold_market_quality_records_total", "gold", "market_quality"),
        ("lakehouse_gold_bars_5m_records_total", "gold", "bars_5m"),
    }

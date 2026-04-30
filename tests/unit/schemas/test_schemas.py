"""Tests for schema definitions."""

from __future__ import annotations

from pyspark.sql.types import StructType

from pipelines.schemas.bronze import (
    ALPACA_BAR_SCHEMA,
    BINANCE_TRADE_SCHEMA,
    YFINANCE_HISTORY_SCHEMA,
)
from pipelines.schemas.gold import (
    GOLD_BARS_AGGREGATED_SCHEMA,
    GOLD_DAILY_VOLUME_SCHEMA,
    GOLD_MARKET_QUALITY_SCHEMA,
)
from pipelines.schemas.partition_spec import partition_columns, zorder_columns
from pipelines.schemas.silver import SILVER_QUARANTINE_TRADES_SCHEMA, SILVER_TRADES_SCHEMA


def test_bronze_schemas_are_structtypes() -> None:
    for schema in [BINANCE_TRADE_SCHEMA, ALPACA_BAR_SCHEMA, YFINANCE_HISTORY_SCHEMA]:
        assert isinstance(schema, StructType)


def test_binance_trade_schema_has_required_fields() -> None:
    fields = {f.name for f in BINANCE_TRADE_SCHEMA.fields}
    assert {"event_time", "symbol", "price", "quantity", "trade_id"} <= fields


def test_silver_quarantine_extends_trades() -> None:
    silver_fields = {f.name for f in SILVER_TRADES_SCHEMA.fields}
    quarantine_fields = {f.name for f in SILVER_QUARANTINE_TRADES_SCHEMA.fields}
    assert (silver_fields - {"event_date"}) <= quarantine_fields
    assert {
        "_error_code",
        "_error_msg",
        "_quarantined_ts",
        "_replay_attempts",
        "_raw_json",
    } <= quarantine_fields


def test_gold_schemas_exist() -> None:
    assert GOLD_BARS_AGGREGATED_SCHEMA.fields
    assert GOLD_DAILY_VOLUME_SCHEMA.fields
    assert GOLD_MARKET_QUALITY_SCHEMA.fields


def test_partition_and_zorder_columns() -> None:
    assert partition_columns("bronze", "binance_trades") == ["ingestion_date"]
    assert partition_columns("silver", "trades") == ["event_date"]
    assert partition_columns("gold", "bars_1m") == ["bar_date"]
    assert zorder_columns("silver", "trades") == ["symbol"]

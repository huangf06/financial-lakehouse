"""Silver bars transform tests."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Row, SparkSession

from pipelines.silver.bars_pipeline import split_bars


def test_split_bars_routes_valid_and_invalid_rows(spark: SparkSession) -> None:
    now = datetime(2026, 4, 30, 12, tzinfo=UTC)
    bronze = spark.createDataFrame(
        [
            Row(
                bar_open_ts=now,
                _ingest_ts=now,
                symbol="AAPL",
                timeframe="1Min",
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1200,
                vwap=100.25,
                trade_count=42,
                _raw_json='{"symbol":"AAPL"}',
            ),
            Row(
                bar_open_ts=now,
                _ingest_ts=now,
                symbol="MSFT",
                timeframe="1Min",
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=-1,
                vwap=100.25,
                trade_count=42,
                _raw_json='{"symbol":"MSFT"}',
            ),
        ]
    )

    passing, quarantined = split_bars(bronze)

    passing_row = passing.select("symbol", "source", "asset_class", "_raw_json").collect()[0]
    quarantined_row = quarantined.selectExpr(
        "symbol",
        "_quality_failures[0].error_code AS error_code",
        "quarantined_date",
        "_replay_attempts",
    ).collect()[0]

    assert passing.count() == 1
    assert passing_row.symbol == "AAPL"
    assert passing_row.source == "alpaca"
    assert passing_row.asset_class == "stock"
    assert passing_row._raw_json == '{"symbol":"AAPL"}'
    assert quarantined.count() == 1
    assert quarantined_row.symbol == "MSFT"
    assert quarantined_row.error_code == "BAR_VOLUME_NEGATIVE"
    assert quarantined_row.quarantined_date is not None
    assert quarantined_row._replay_attempts == 0

"""Trade rule smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from pipelines.quality import QualityFramework
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES


def test_negative_price_quarantined(spark: SparkSession) -> None:
    now = datetime(2026, 4, 30, 12, tzinfo=UTC)
    df = spark.createDataFrame(
        [(now, now, "BTCUSDT", Decimal("-1"), Decimal("1"), "binance:1")],
        ["event_ts", "ingest_ts", "symbol", "price", "quantity", "trade_id"],
    )
    assert QualityFramework(TRADE_RULES).split(df)[1].count() == 1

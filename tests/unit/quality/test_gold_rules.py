"""Gold rule smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from pipelines.quality import QualityFramework
from pipelines.quality.rules.silver_to_gold_bars import GOLD_BAR_RULES


def test_gold_negative_volume_quarantined(spark: SparkSession) -> None:
    now = datetime(2026, 4, 30, 12, tzinfo=UTC)
    df = spark.createDataFrame(
        [
            (
                now,
                "AAPL",
                "1m",
                Decimal("1"),
                Decimal("2"),
                Decimal("1"),
                Decimal("2"),
                Decimal("-1"),
            )
        ],
        ["bar_open_ts", "symbol", "timeframe", "open", "high", "low", "close", "volume"],
    )
    assert QualityFramework(GOLD_BAR_RULES).split(df)[1].count() == 1

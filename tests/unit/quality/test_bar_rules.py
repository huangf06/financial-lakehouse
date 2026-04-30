"""Bar rule smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from pipelines.quality import QualityFramework
from pipelines.quality.rules.bronze_to_silver_bars import BAR_RULES


def test_negative_volume_quarantined(spark: SparkSession) -> None:
    now = datetime(2026, 4, 30, 12, tzinfo=UTC)
    df = spark.createDataFrame(
        [
            (
                now,
                now,
                "AAPL",
                "1Min",
                Decimal("1"),
                Decimal("2"),
                Decimal("1"),
                Decimal("2"),
                Decimal("-1"),
            )
        ],
        [
            "bar_open_ts",
            "ingest_ts",
            "symbol",
            "timeframe",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
    )
    assert QualityFramework(BAR_RULES).split(df)[1].count() == 1

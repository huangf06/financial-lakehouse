"""Gold market quality metrics schema."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOLD_MARKET_QUALITY_SCHEMA = StructType(
    [
        StructField("metric_hour", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("asset_class", StringType(), False),
        StructField("trade_count", LongType(), False),
        StructField("late_arrival_pct", DoubleType(), True),
        StructField("quarantine_pct", DoubleType(), True),
        StructField("price_volatility", DoubleType(), True),
        StructField("price_max", DoubleType(), True),
        StructField("price_min", DoubleType(), True),
    ]
)

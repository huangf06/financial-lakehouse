"""yfinance historical bar payload schema."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

YFINANCE_HISTORY_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("bar_open_ts", TimestampType(), False),
        StructField("timeframe", StringType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ]
)

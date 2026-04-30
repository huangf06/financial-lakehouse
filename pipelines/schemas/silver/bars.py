"""Silver bars unified schema."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_BARS_SCHEMA = StructType(
    [
        StructField("bar_open_ts", TimestampType(), False),
        StructField("ingest_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("source", StringType(), False),
        StructField("asset_class", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("timeframe", StringType(), False),
        StructField("open", DecimalType(38, 18), True),
        StructField("high", DecimalType(38, 18), True),
        StructField("low", DecimalType(38, 18), True),
        StructField("close", DecimalType(38, 18), True),
        StructField("volume", DecimalType(38, 18), True),
        StructField("vwap", DecimalType(38, 18), True),
        StructField("trade_count", LongType(), True),
    ]
)

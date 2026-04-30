"""Silver trades unified schema."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_TRADES_SCHEMA = StructType(
    [
        StructField("event_ts", TimestampType(), False),
        StructField("ingest_ts", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("source", StringType(), False),
        StructField("asset_class", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("side", StringType(), True),
        StructField("price", DecimalType(38, 18), False),
        StructField("quantity", DecimalType(38, 18), False),
        StructField("notional", DecimalType(38, 18), False),
        StructField("trade_id", StringType(), False),
        StructField("late_arrival_sec", IntegerType(), True),
    ]
)

"""Binance WS trade payload schema."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BINANCE_TRADE_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("trade_time", TimestampType(), True),
        StructField("buyer_is_maker", BooleanType(), True),
    ]
)

"""Gold daily volume profile schema."""

from __future__ import annotations

from pyspark.sql.types import DateType, DecimalType, LongType, StringType, StructField, StructType

GOLD_DAILY_VOLUME_SCHEMA = StructType(
    [
        StructField("bar_date", DateType(), False),
        StructField("symbol", StringType(), False),
        StructField("asset_class", StringType(), False),
        StructField("total_volume", DecimalType(38, 18), False),
        StructField("total_notional", DecimalType(38, 18), False),
        StructField("trade_count", LongType(), False),
        StructField("vwap", DecimalType(38, 18), False),
        StructField("price_high", DecimalType(38, 18), False),
        StructField("price_low", DecimalType(38, 18), False),
    ]
)

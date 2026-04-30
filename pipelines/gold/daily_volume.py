"""Gold daily volume profile."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.functions import max as sf_max
from pyspark.sql.functions import min as sf_min
from pyspark.sql.functions import sum as sf_sum


def daily_volume_profile(trades: DataFrame) -> DataFrame:
    return (
        trades.groupBy(col("event_date").alias("bar_date"), "symbol", "asset_class")
        .agg(
            sf_sum("quantity").alias("total_volume"),
            sf_sum("notional").alias("total_notional"),
            count("*").alias("trade_count"),
            sf_max("price").alias("price_high"),
            sf_min("price").alias("price_low"),
        )
        .withColumn("vwap", col("total_notional") / col("total_volume"))
    )

"""Roll up trades to one-minute bars."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, date_trunc, first, last, lit, to_date
from pyspark.sql.functions import max as sf_max
from pyspark.sql.functions import min as sf_min
from pyspark.sql.functions import sum as sf_sum


def trades_to_1m_bars(trades: DataFrame) -> DataFrame:
    bucket = date_trunc("minute", col("event_ts"))
    return (
        trades.withColumn("bar_open_ts", bucket)
        .groupBy("bar_open_ts", "symbol", "asset_class")
        .agg(
            first("price", ignorenulls=True).alias("open"),
            sf_max("price").alias("high"),
            sf_min("price").alias("low"),
            last("price", ignorenulls=True).alias("close"),
            sf_sum("quantity").alias("volume"),
            count("*").alias("trade_count"),
            (sf_sum("notional") / sf_sum("quantity")).alias("vwap"),
        )
        .withColumn("event_date", to_date("bar_open_ts"))
        .withColumn("source", lit("trade_rollup"))
        .withColumn("timeframe", lit("1m"))
    )

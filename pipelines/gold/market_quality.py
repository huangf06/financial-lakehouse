"""Gold market quality metrics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, date_trunc, stddev
from pyspark.sql.functions import max as sf_max
from pyspark.sql.functions import min as sf_min


def market_quality(trades: DataFrame) -> DataFrame:
    return (
        trades.withColumn("metric_hour", date_trunc("hour", col("event_ts")))
        .groupBy("metric_hour", "symbol", "asset_class")
        .agg(
            count("*").alias("trade_count"),
            avg((col("late_arrival_sec") > 60).cast("double")).alias("late_arrival_pct"),
            stddev("price").cast("double").alias("price_volatility"),
            sf_max("price").cast("double").alias("price_max"),
            sf_min("price").cast("double").alias("price_min"),
        )
        .withColumn("quarantine_pct", col("late_arrival_pct") * 0.0)
    )

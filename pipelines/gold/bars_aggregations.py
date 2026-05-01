"""Gold bar aggregations."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    floor,
    from_unixtime,
    lit,
    max_by,
    min_by,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.sql.functions import max as sf_max
from pyspark.sql.functions import min as sf_min
from pyspark.sql.functions import sum as sf_sum

from pipelines.quality import QualityFramework
from pipelines.quality.rules.silver_to_gold_bars import GOLD_BAR_RULES

_TRUNC_BY_TIMEFRAME = {"1h": "hour", "1d": "day"}


def _bucket_start(timeframe: str) -> Column:
    if timeframe == "5m":
        seconds = floor(unix_timestamp(col("bar_open_ts")) / lit(300)) * lit(300)
        return from_unixtime(seconds).cast("timestamp")
    if timeframe in _TRUNC_BY_TIMEFRAME:
        return date_trunc(_TRUNC_BY_TIMEFRAME[timeframe], col("bar_open_ts"))
    raise ValueError(f"Unsupported bar timeframe: {timeframe}")


def aggregate_bars(bars_1m: DataFrame, timeframe: str) -> DataFrame:
    if timeframe == "1m":
        return bars_1m.withColumn("bar_date", to_date("bar_open_ts"))
    aggregated = (
        bars_1m.withColumn("_source_bar_open_ts", col("bar_open_ts"))
        .withColumn("bar_open_ts", _bucket_start(timeframe))
        .groupBy("bar_open_ts", "symbol", "asset_class")
        .agg(
            min_by("open", "_source_bar_open_ts").alias("open"),
            sf_max("high").alias("high"),
            sf_min("low").alias("low"),
            max_by("close", "_source_bar_open_ts").alias("close"),
            sf_sum("volume").alias("volume"),
            (
                sf_sum(col("vwap") * col("volume"))
                / when(sf_sum("volume") == 0, lit(None)).otherwise(sf_sum("volume"))
            ).alias("vwap"),
            sf_sum("trade_count").alias("trade_count"),
        )
        .withColumn("timeframe", lit(timeframe))
        .withColumn("bar_date", to_date("bar_open_ts"))
    )
    return QualityFramework(GOLD_BAR_RULES).split(aggregated)[0]

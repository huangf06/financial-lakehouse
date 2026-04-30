"""Gold bar aggregations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc, first, last, lit, to_date
from pyspark.sql.functions import max as sf_max
from pyspark.sql.functions import min as sf_min
from pyspark.sql.functions import sum as sf_sum

from pipelines.quality import QualityFramework
from pipelines.quality.rules.silver_to_gold_bars import GOLD_BAR_RULES

_TRUNC_BY_TIMEFRAME = {"5m": "minute", "1h": "hour", "1d": "day"}


def aggregate_bars(bars_1m: DataFrame, timeframe: str) -> DataFrame:
    if timeframe == "1m":
        return bars_1m.withColumn("bar_date", to_date("bar_open_ts"))
    trunc_unit = _TRUNC_BY_TIMEFRAME[timeframe]
    aggregated = (
        bars_1m.withColumn("bar_open_ts", date_trunc(trunc_unit, col("bar_open_ts")))
        .groupBy("bar_open_ts", "symbol", "asset_class")
        .agg(
            first("open", ignorenulls=True).alias("open"),
            sf_max("high").alias("high"),
            sf_min("low").alias("low"),
            last("close", ignorenulls=True).alias("close"),
            sf_sum("volume").alias("volume"),
            sf_sum("trade_count").alias("trade_count"),
        )
        .withColumn("timeframe", lit(timeframe))
        .withColumn("bar_date", to_date("bar_open_ts"))
    )
    return QualityFramework(GOLD_BAR_RULES).split(aggregated)[0]

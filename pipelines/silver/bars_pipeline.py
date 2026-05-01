"""Silver bars transform."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_date

from pipelines.quality import QualityFramework
from pipelines.quality.rules.bronze_to_silver_bars import BAR_RULES


def normalize_alpaca_bars(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        col("bar_open_ts").cast("timestamp").alias("bar_open_ts"),
        col("_ingest_ts").cast("timestamp").alias("ingest_ts"),
        to_date("bar_open_ts").alias("event_date"),
        lit("alpaca").alias("source"),
        lit("stock").alias("asset_class"),
        col("symbol"),
        col("timeframe"),
        col("open").cast("decimal(38,18)").alias("open"),
        col("high").cast("decimal(38,18)").alias("high"),
        col("low").cast("decimal(38,18)").alias("low"),
        col("close").cast("decimal(38,18)").alias("close"),
        col("volume").cast("decimal(38,18)").alias("volume"),
        col("vwap").cast("decimal(38,18)").alias("vwap"),
        col("trade_count").cast("long").alias("trade_count"),
        (col("_raw_json") if "_raw_json" in bronze.columns else lit(None)).alias("_raw_json"),
    )


def split_bars(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    normalized = normalize_alpaca_bars(bronze)
    passing, quarantined = QualityFramework(BAR_RULES).split(normalized)
    quarantined = (
        quarantined.withColumn("_quarantined_ts", current_timestamp())
        .withColumn("quarantined_date", to_date("_quarantined_ts"))
        .withColumn("_replay_attempts", lit(0))
    )
    return passing, quarantined

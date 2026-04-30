"""Silver trades transform."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, current_timestamp, lit, to_date, when

from pipelines.quality import QualityFramework
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES


def normalize_binance_trades(bronze: DataFrame) -> DataFrame:
    price = col("price").cast("decimal(38,18)")
    quantity = col("quantity").cast("decimal(38,18)")
    event_ts = col("trade_time").cast("timestamp")
    ingest_ts = col("_ingest_ts").cast("timestamp")
    return bronze.select(
        event_ts.alias("event_ts"),
        ingest_ts.alias("ingest_ts"),
        to_date(event_ts).alias("event_date"),
        lit("binance").alias("source"),
        lit("crypto").alias("asset_class"),
        col("symbol"),
        when(col("buyer_is_maker"), lit("sell")).otherwise(lit("buy")).alias("side"),
        price.alias("price"),
        quantity.alias("quantity"),
        (price * quantity).alias("notional"),
        concat(lit("binance:"), col("trade_id").cast("string")).alias("trade_id"),
        (ingest_ts.cast("long") - event_ts.cast("long")).cast("int").alias("late_arrival_sec"),
        (col("_raw_json") if "_raw_json" in bronze.columns else lit(None)).alias("_raw_json"),
    )


def split_trades(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    normalized = normalize_binance_trades(bronze)
    passing, quarantined = QualityFramework(TRADE_RULES).split(normalized)
    quarantined = (
        quarantined.withColumn("_quarantined_ts", current_timestamp())
        .withColumn("quarantined_date", to_date("_quarantined_ts"))
        .withColumn("_replay_attempts", lit(0))
    )
    return passing, quarantined

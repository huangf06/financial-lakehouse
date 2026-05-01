"""Spark smoke test reading trades-derived Gold Delta tables."""

from __future__ import annotations

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from jobs._common import spark_session
from pipelines.config import load_settings


def _count_delta(spark: SparkSession, path: str, label: str) -> int:
    try:
        count = spark.read.format("delta").load(path).count()
    except Py4JJavaError as exc:
        if "DELTA_TABLE_NOT_FOUND" not in str(exc) and "Path does not exist" not in str(exc):
            raise
        print(f"=== {label}: table not found ===")
        return 0
    print(f"=== {label}: {count} records ===")
    return count


def main() -> None:
    settings = load_settings()
    spark = spark_session("gold-count", settings)
    spark.sparkContext.setLogLevel("WARN")

    daily_volume_path = settings.table_path("gold", "daily_volume_profile")
    market_quality_path = settings.table_path("gold", "market_quality")

    daily_count = _count_delta(spark, daily_volume_path, "GOLD DAILY VOLUME COUNT")
    market_count = _count_delta(spark, market_quality_path, "GOLD MARKET QUALITY COUNT")

    if daily_count:
        spark.read.format("delta").load(daily_volume_path).select(
            "bar_date", "symbol", "trade_count", "total_volume", "vwap"
        ).show(5, truncate=False)
    if market_count:
        spark.read.format("delta").load(market_quality_path).select(
            "metric_hour", "symbol", "trade_count", "late_arrival_pct", "quarantine_pct"
        ).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

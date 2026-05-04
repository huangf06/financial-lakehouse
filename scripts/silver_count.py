"""Spark smoke test reading Silver Delta tables."""

from __future__ import annotations

from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from jobs._common import spark_session
from pipelines.config import load_settings


def _count_delta(spark: SparkSession, path: str, label: str) -> int:
    try:
        count = spark.read.format("delta").load(path).count()
    except (Py4JJavaError, AnalysisException) as exc:
        message = str(exc)
        if (
            "DELTA_TABLE_NOT_FOUND" not in message
            and "PATH_NOT_FOUND" not in message
            and "Path does not exist" not in message
        ):
            raise
        print(f"=== {label}: table not found ===")
        return 0
    print(f"=== {label}: {count} records ===")
    return count


def main() -> None:
    settings = load_settings()
    spark = spark_session("silver-count", settings)
    spark.sparkContext.setLogLevel("WARN")
    silver_path = settings.table_path("silver", "trades")
    quarantine_path = settings.table_path("silver", "quarantine_trades")
    bars_path = settings.table_path("silver", "bars")
    bars_quarantine_path = settings.table_path("silver", "quarantine_bars")

    silver_count = _count_delta(spark, silver_path, "SILVER TRADES COUNT")
    quarantine_count = _count_delta(spark, quarantine_path, "SILVER QUARANTINE COUNT")
    bars_count = _count_delta(spark, bars_path, "SILVER BARS COUNT")
    bars_quarantine_count = _count_delta(
        spark, bars_quarantine_path, "SILVER BARS QUARANTINE COUNT"
    )

    if silver_count:
        spark.read.format("delta").load(silver_path).select(
            "trade_id", "symbol", "event_date", "price", "quantity"
        ).show(5, truncate=False)
    if quarantine_count:
        spark.read.format("delta").load(quarantine_path).select(
            "trade_id", "symbol", "quarantined_date", "_quality_failures"
        ).show(5, truncate=False)
    if bars_count:
        spark.read.format("delta").load(bars_path).select(
            "bar_open_ts", "symbol", "timeframe", "open", "close", "volume"
        ).show(5, truncate=False)
    if bars_quarantine_count:
        spark.read.format("delta").load(bars_quarantine_path).select(
            "bar_open_ts", "symbol", "quarantined_date", "_quality_failures"
        ).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

"""Spark smoke test reading Silver trades and quarantine Delta tables."""

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
    spark = spark_session("silver-count", settings)
    spark.sparkContext.setLogLevel("WARN")
    silver_path = settings.table_path("silver", "trades")
    quarantine_path = settings.table_path("silver", "quarantine_trades")

    silver_count = _count_delta(spark, silver_path, "SILVER TRADES COUNT")
    quarantine_count = _count_delta(spark, quarantine_path, "SILVER QUARANTINE COUNT")

    if silver_count:
        spark.read.format("delta").load(silver_path).select(
            "trade_id", "symbol", "event_date", "price", "quantity"
        ).show(5, truncate=False)
    if quarantine_count:
        spark.read.format("delta").load(quarantine_path).select(
            "trade_id", "symbol", "quarantined_date", "_quality_failures"
        ).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

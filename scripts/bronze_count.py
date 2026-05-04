"""Spark smoke test reading Bronze Delta tables from MinIO."""

from __future__ import annotations

from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from jobs._common import spark_session
from pipelines.config import load_settings


def _read_delta_if_exists(spark: SparkSession, path: str, label: str) -> DataFrame | None:
    try:
        return spark.read.format("delta").load(path)
    except (Py4JJavaError, AnalysisException) as exc:
        message = str(exc)
        if (
            "DELTA_TABLE_NOT_FOUND" not in message
            and "PATH_NOT_FOUND" not in message
            and "Path does not exist" not in message
        ):
            raise
        print(f"=== {label}: table not found ===")
        return None


def _show_table(df: DataFrame | None, label: str, columns: list[str]) -> int:
    if df is None:
        return 0
    count = df.count()
    print(f"=== {label}: {count} records ===")
    if count:
        df.select(*columns).show(5, truncate=False)
    return count


def main() -> None:
    settings = load_settings()
    spark = spark_session("bronze-count", settings)
    spark.sparkContext.setLogLevel("WARN")

    trades = _read_delta_if_exists(
        spark, settings.table_path("bronze", "binance_trades"), "BRONZE BINANCE COUNT"
    )
    bars = _read_delta_if_exists(
        spark, settings.table_path("bronze", "alpaca_bars"), "BRONZE ALPACA BARS COUNT"
    )

    _show_table(
        trades,
        "BRONZE BINANCE COUNT",
        ["symbol", "trade_id", "_source", "ingestion_date", "_file_path"],
    )
    _show_table(
        bars,
        "BRONZE ALPACA BARS COUNT",
        ["symbol", "bar_open_ts", "timeframe", "_source", "ingestion_date", "_file_path"],
    )
    spark.stop()


if __name__ == "__main__":
    main()

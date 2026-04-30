"""Spark job: Bronze Binance trades to Silver trades + quarantine."""

from __future__ import annotations

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.silver.trades_pipeline import split_trades


def main() -> None:
    settings = load_settings()
    spark = spark_session("silver-trades", settings)
    bronze_path = settings.table_path("bronze", "binance_trades")
    silver_path = settings.table_path("silver", "trades")
    quarantine_path = settings.table_path("silver", "quarantine_trades")
    passing, quarantined = split_trades(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"])
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

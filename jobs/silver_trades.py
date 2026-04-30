"""Spark job: Bronze Binance trades to Silver trades + quarantine."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.silver.trades_pipeline import split_trades


def main() -> None:
    spark = spark_session("silver-trades")
    bronze_path = os.environ["BRONZE_BINANCE_TRADES_PATH"]
    silver_path = os.environ["SILVER_TRADES_PATH"]
    quarantine_path = os.environ["SILVER_QUARANTINE_TRADES_PATH"]
    passing, quarantined = split_trades(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"])
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

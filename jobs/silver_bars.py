"""Spark job: Bronze Alpaca bars to Silver bars + quarantine."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.silver.bars_pipeline import split_bars


def main() -> None:
    spark = spark_session("silver-bars")
    bronze_path = os.environ["BRONZE_ALPACA_BARS_PATH"]
    silver_path = os.environ["SILVER_BARS_PATH"]
    quarantine_path = os.environ["SILVER_QUARANTINE_BARS_PATH"]
    passing, quarantined = split_bars(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"])
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

"""Spark job: Bronze Alpaca bars to Silver bars + quarantine."""

from __future__ import annotations

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.silver.bars_pipeline import split_bars


def main() -> None:
    settings = load_settings()
    spark = spark_session("silver-bars", settings)
    bronze_path = settings.table_path("bronze", "alpaca_bars")
    silver_path = settings.table_path("silver", "bars")
    quarantine_path = settings.table_path("silver", "quarantine_bars")
    passing, quarantined = split_bars(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"])
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

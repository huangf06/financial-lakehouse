"""Spark job: Silver trades to Gold daily volume profile."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.gold.daily_volume import daily_volume_profile


def main() -> None:
    spark = spark_session("daily-volume-profile")
    write_delta(
        daily_volume_profile(read_delta(spark, os.environ["SILVER_TRADES_PATH"])),
        os.environ["GOLD_DAILY_VOLUME_PROFILE_PATH"],
        ["bar_date"],
    )
    spark.stop()


if __name__ == "__main__":
    main()

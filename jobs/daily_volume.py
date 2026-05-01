"""Spark job: Silver trades to Gold daily volume profile."""

from __future__ import annotations

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.gold.daily_volume import daily_volume_profile


def main() -> None:
    settings = load_settings()
    spark = spark_session("daily-volume-profile", settings)
    write_delta(
        daily_volume_profile(read_delta(spark, settings.table_path("silver", "trades"))),
        settings.table_path("gold", "daily_volume_profile"),
        ["bar_date"],
        mode="overwrite",
    )
    spark.stop()


if __name__ == "__main__":
    main()

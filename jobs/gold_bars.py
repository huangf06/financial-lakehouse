"""Spark job: Silver bars to Gold bars."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.gold.bars_aggregations import aggregate_bars


def main() -> None:
    settings = load_settings()
    spark = spark_session("gold-bars", settings)
    silver_path = settings.table_path("silver", "bars")
    timeframe = os.environ.get("GOLD_TIMEFRAME", "1m")
    gold_path = settings.table_path("gold", f"bars_{timeframe}")
    write_delta(aggregate_bars(read_delta(spark, silver_path), timeframe), gold_path, ["bar_date"])
    spark.stop()


if __name__ == "__main__":
    main()

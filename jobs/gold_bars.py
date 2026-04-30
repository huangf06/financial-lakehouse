"""Spark job: Silver bars to Gold bars."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.gold.bars_aggregations import aggregate_bars


def main() -> None:
    spark = spark_session("gold-bars")
    silver_path = os.environ["SILVER_BARS_PATH"]
    timeframe = os.environ.get("GOLD_TIMEFRAME", "1m")
    gold_path = os.environ[f"GOLD_BARS_{timeframe.upper()}_PATH"]
    write_delta(aggregate_bars(read_delta(spark, silver_path), timeframe), gold_path, ["bar_date"])
    spark.stop()


if __name__ == "__main__":
    main()

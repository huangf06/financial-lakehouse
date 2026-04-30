"""Spark job: Silver trades to Gold market quality metrics."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.gold.market_quality import market_quality


def main() -> None:
    spark = spark_session("market-quality")
    write_delta(
        market_quality(read_delta(spark, os.environ["SILVER_TRADES_PATH"])),
        os.environ["GOLD_MARKET_QUALITY_PATH"],
        ["metric_hour"],
    )
    spark.stop()


if __name__ == "__main__":
    main()

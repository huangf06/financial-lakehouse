"""Spark job: Silver trades to Gold market quality metrics."""

from __future__ import annotations

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.gold.market_quality import market_quality


def main() -> None:
    settings = load_settings()
    spark = spark_session("market-quality", settings)
    write_delta(
        market_quality(read_delta(spark, settings.table_path("silver", "trades"))),
        settings.table_path("gold", "market_quality"),
        ["metric_hour"],
        mode="overwrite",
    )
    spark.stop()


if __name__ == "__main__":
    main()

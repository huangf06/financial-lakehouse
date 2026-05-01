"""Local Silver/Gold bars evidence using real Delta reads and writes."""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import Row

from jobs._common import read_delta, spark_session, write_delta
from pipelines.bronze import annotate_bronze
from pipelines.config import load_settings
from pipelines.gold.bars_aggregations import aggregate_bars
from pipelines.schemas.bronze import ALPACA_BAR_SCHEMA
from pipelines.silver.bars_pipeline import split_bars


def main() -> None:
    settings = load_settings()
    spark = spark_session("bars-demo", settings)
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = settings.table_path("demo", "bronze_alpaca_bars")
    silver_path = settings.table_path("demo", "silver_bars")
    quarantine_path = settings.table_path("demo", "silver_quarantine_bars")
    gold_path = settings.table_path("demo", "gold_bars_5m")

    start = datetime(2026, 4, 30, 12, 0)
    valid_rows = [
        Row(
            symbol="AAPL",
            bar_open_ts=start + timedelta(minutes=i),
            timeframe="1m",
            open=100.0 + i,
            high=102.0 + i,
            low=99.0 + i,
            close=101.0 + i,
            volume=1000 + i,
            vwap=100.5 + i,
            trade_count=10 + i,
        )
        for i in range(5)
    ]
    invalid_row = Row(
        symbol="AAPL",
        bar_open_ts=start + timedelta(minutes=5),
        timeframe="1m",
        open=105.0,
        high=106.0,
        low=104.0,
        close=105.5,
        volume=-1,
        vwap=105.25,
        trade_count=10,
    )
    bronze = spark.createDataFrame([*valid_rows, invalid_row], ALPACA_BAR_SCHEMA)
    write_delta(annotate_bronze(bronze, "alpaca"), bronze_path, mode="overwrite")

    passing, quarantined = split_bars(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"], mode="overwrite")
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"], mode="overwrite")

    bars_5m = aggregate_bars(read_delta(spark, silver_path), "5m")
    write_delta(bars_5m, gold_path, partition_by=["bar_date"], mode="overwrite")

    silver_count = read_delta(spark, silver_path).count()
    quarantine_count = read_delta(spark, quarantine_path).count()
    gold_count = read_delta(spark, gold_path).count()
    gold_rows = [
        row.asDict()
        for row in read_delta(spark, gold_path)
        .select("bar_open_ts", "symbol", "timeframe", "open", "high", "low", "close", "volume")
        .collect()
    ]

    print("=== BARS DEMO ===")
    print(f"silver_bars={silver_count}")
    print(f"quarantine_bars={quarantine_count}")
    print(f"gold_bars_5m={gold_count}")
    print(f"gold_rows={gold_rows}")

    if silver_count != 5 or quarantine_count != 1 or gold_count != 1:
        raise RuntimeError("Bars demo did not produce the expected Silver/Quarantine/Gold counts")

    spark.stop()


if __name__ == "__main__":
    main()

"""Spark smoke test reading Bronze Binance Delta from MinIO."""

from __future__ import annotations

from jobs._common import spark_session
from pipelines.config import load_settings


def main() -> None:
    settings = load_settings()
    spark = spark_session("bronze-count", settings)
    spark.sparkContext.setLogLevel("WARN")
    table_path = settings.table_path("bronze", "binance_trades")
    df = spark.read.format("delta").load(table_path)
    count = df.count()
    print(f"=== BRONZE COUNT: {count} records ===")
    if count == 0:
        print("Bronze table has no data yet.")
        spark.stop()
        return
    df.select("symbol", "trade_id", "_source", "ingestion_date", "_file_path").show(
        5, truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()

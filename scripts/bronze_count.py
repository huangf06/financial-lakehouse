"""Spark smoke test reading Bronze Binance Delta from MinIO."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("bronze-count")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", os.environ.get("S3_PATH_STYLE", "true"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    table_path = os.environ.get(
        "BRONZE_BINANCE_TRADES_PATH", "s3a://lakehouse/bronze/binance_trades"
    )
    df = spark.read.format("delta").load(table_path)
    count = df.count()
    print(f"=== BRONZE COUNT: {count} records ===")
    assert count > 0
    df.select("symbol", "trade_id", "_source", "ingestion_date", "_file_path").show(
        5, truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()

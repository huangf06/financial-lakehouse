"""Spark smoke test reading landing/binance from MinIO."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("smoke-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", os.environ.get("S3_PATH_STYLE", "true"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("recursiveFileLookup", "true").json("s3a://lakehouse/landing/binance/")
    count = df.count()
    print(f"=== SMOKE TEST: read {count} records ===")
    assert count > 0
    df.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

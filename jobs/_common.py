"""Shared Spark job helpers."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", os.environ.get("S3_PATH_STYLE", "true"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

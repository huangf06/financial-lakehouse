"""Shared Spark job helpers."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from pipelines.config import Settings, load_settings


def spark_session(app_name: str, settings: Settings | None = None) -> SparkSession:
    resolved_settings = settings or load_settings()
    storage = resolved_settings.storage
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", resolved_settings.spark.master))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", storage.endpoint or "")
        .config("spark.hadoop.fs.s3a.access.key", storage.access_key or "")
        .config("spark.hadoop.fs.s3a.secret.key", storage.secret_key or "")
        .config("spark.hadoop.fs.s3a.path.style.access", str(storage.path_style_access).lower())
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

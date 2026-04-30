"""Bronze layer streaming reader with Databricks and OSS Spark branches."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def bronze_stream_reader(
    spark: SparkSession,
    source: str,
    landing_path: str,
    schema_path: str,
    initial_schema: StructType,
) -> DataFrame:
    if is_databricks():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(landing_path)
        )
    return (
        spark.readStream.format("json")
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .schema(initial_schema)
        .load(landing_path)
    )

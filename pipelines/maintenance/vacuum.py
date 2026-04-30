"""Delta vacuum helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession


def vacuum_sql(table_identifier: str, retain_hours: int) -> str:
    return f"VACUUM {table_identifier} RETAIN {retain_hours} HOURS"


def run_vacuum(spark: SparkSession, table_identifier: str, retain_hours: int) -> None:
    spark.sql(vacuum_sql(table_identifier, retain_hours))

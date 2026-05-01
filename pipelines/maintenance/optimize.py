"""Delta optimization helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipelines.schemas.partition_spec import zorder_columns


def delta_path_identifier(table_path: str) -> str:
    return f"delta.`{table_path}`"


def optimize_sql(table_identifier: str, layer: str, table: str, where: str | None = None) -> str:
    predicate = f" WHERE {where}" if where else ""
    columns = zorder_columns(layer, table)
    zorder = f" ZORDER BY ({', '.join(columns)})" if columns else ""
    return f"OPTIMIZE {table_identifier}{predicate}{zorder}"


def run_optimize(
    spark: SparkSession, table_identifier: str, layer: str, table: str, where: str | None = None
) -> None:
    spark.sql(optimize_sql(table_identifier, layer, table, where))

"""Bronze writer helpers."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    struct,
    to_date,
    to_json,
)


def annotate_bronze(df: DataFrame, source: str) -> DataFrame:
    source_columns = [col(name) for name in df.columns]
    return (
        df.withColumn("_raw_json", to_json(struct(*source_columns)))
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source", lit(source))
        .withColumn("_file_path", input_file_name())
        .withColumn("ingestion_date", to_date("_ingest_ts"))
    )


def write_bronze_stream(
    df: DataFrame,
    *,
    source: str,
    table_path: str,
    checkpoint_location: str,
    trigger_seconds: int = 30,
    available_now: bool = False,
) -> Any:
    writer = (
        annotate_bronze(df, source)
        .writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")
    return writer.start(table_path)

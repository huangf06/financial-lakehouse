"""Quarantine replay helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, current_timestamp, lit

from pipelines.quality import QualityFramework


def prepare_quarantine(df: DataFrame, raw_json_col: str = "_raw_json") -> DataFrame:
    return (
        df.withColumn("_quarantined_ts", current_timestamp())
        .withColumn("quarantined_date", current_date())
        .withColumn("_replay_attempts", lit(0))
        .withColumn("_raw_json", col(raw_json_col) if raw_json_col in df.columns else lit(None))
    )


def replay_quarantine(df: DataFrame, framework: QualityFramework) -> tuple[DataFrame, DataFrame]:
    eligible = df.filter(col("_replay_attempts") < 3)
    passing, still_failing = framework.split(eligible.drop("_quality_failures"))
    return passing, still_failing.withColumn("_replay_attempts", col("_replay_attempts") + 1)

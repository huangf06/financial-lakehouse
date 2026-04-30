"""QualityFramework unit tests."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipelines.quality import QualityFramework, Rule


def test_split_separates_passing_and_quarantined(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 100.0), (2, -1.0), (3, 50.0)], ["id", "price"])
    fw = QualityFramework(
        [Rule("BR-001", "PRICE_NOT_POSITIVE", "error", "price > 0", lambda df: col("price") > 0)]
    )
    passing, quarantined = fw.split(df)
    assert passing.count() == 2
    assert quarantined.count() == 1
    assert quarantined.collect()[0]["_quality_failures"][0]["error_code"] == "PRICE_NOT_POSITIVE"


def test_warning_does_not_quarantine(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 30), (2, 120)], ["id", "late_sec"])
    fw = QualityFramework(
        [Rule("BR-007", "LATE_ARRIVAL", "warning", "late", lambda df: col("late_sec") < 60)]
    )
    passing, quarantined = fw.split(df)
    assert passing.count() == 2
    assert quarantined.count() == 0

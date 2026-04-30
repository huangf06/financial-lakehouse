"""Quarantine replay integration evidence."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipelines.quality import QualityFramework, Rule
from pipelines.quality.replay import replay_quarantine


@pytest.mark.integration
def test_quarantine_replay_splits_now_passing_and_still_failing(spark: SparkSession) -> None:
    schema = (
        "id int, price double, _replay_attempts int, "
        "_quality_failures array<struct<error_code:string,error_msg:string,severity:string>>"
    )
    quarantined = spark.createDataFrame(
        [
            (1, 100.0, 0, [{"error_code": "OLD_RULE", "error_msg": "old", "severity": "error"}]),
            (2, -1.0, 0, [{"error_code": "OLD_RULE", "error_msg": "old", "severity": "error"}]),
            (3, 50.0, 3, [{"error_code": "OLD_RULE", "error_msg": "old", "severity": "error"}]),
        ],
        schema,
    )
    framework = QualityFramework(
        [Rule("NEW-001", "PRICE_NOT_POSITIVE", "error", "price > 0", lambda df: col("price") > 0)]
    )

    now_passing, still_failing = replay_quarantine(quarantined, framework)

    assert [row.id for row in now_passing.select("id").collect()] == [1]
    assert [row.id for row in still_failing.select("id").collect()] == [2]
    assert still_failing.collect()[0]["_replay_attempts"] == 1

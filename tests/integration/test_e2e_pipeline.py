"""Bronze-to-Silver quarantine replay evidence."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col

from jobs._common import read_delta, write_delta
from pipelines.bronze import annotate_bronze
from pipelines.quality import QualityFramework, Rule
from pipelines.quality.replay import replay_quarantine
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA
from pipelines.silver.trades_pipeline import split_trades


def _rules_without_symbol_allowlist() -> list[Rule]:
    return [rule for rule in TRADE_RULES if rule.error_code != "SYMBOL_UNKNOWN"]


@pytest.mark.integration
def test_bronze_to_silver_quarantine_replay_delta_loop(
    spark: SparkSession, table_path: str
) -> None:
    bronze_path = f"{table_path}_bronze"
    silver_path = f"{table_path}_silver"
    quarantine_path = f"{table_path}_quarantine"
    event_ts = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)

    bronze = spark.createDataFrame(
        [
            Row(
                event_type="trade",
                event_time=event_ts,
                symbol="DEMOUSDT",
                trade_id=9001,
                price="100.00",
                quantity="1.5",
                trade_time=event_ts,
                buyer_is_maker=False,
            )
        ],
        BINANCE_TRADE_SCHEMA,
    )
    write_delta(annotate_bronze(bronze, "binance"), bronze_path, mode="overwrite")

    passing, quarantined = split_trades(read_delta(spark, bronze_path))
    write_delta(passing, silver_path, partition_by=["event_date"], mode="overwrite")
    write_delta(quarantined, quarantine_path, partition_by=["quarantined_date"], mode="overwrite")

    assert read_delta(spark, silver_path).count() == 0
    assert read_delta(spark, quarantine_path).count() == 1
    assert (
        read_delta(spark, quarantine_path)
        .selectExpr("_quality_failures[0].error_code AS error_code")
        .collect()[0]
        .error_code
        == "SYMBOL_UNKNOWN"
    )

    now_passing, still_failing = replay_quarantine(
        read_delta(spark, quarantine_path),
        QualityFramework(_rules_without_symbol_allowlist()),
    )
    write_delta(now_passing, silver_path, partition_by=["event_date"], mode="append")
    write_delta(still_failing, quarantine_path, partition_by=["quarantined_date"], mode="overwrite")

    silver = read_delta(spark, silver_path)
    quarantine = read_delta(spark, quarantine_path)
    assert silver.count() == 1
    assert quarantine.count() == 0
    assert (
        silver.filter(col("trade_id") == "binance:9001").select("symbol").collect()[0].symbol
        == "DEMOUSDT"
    )

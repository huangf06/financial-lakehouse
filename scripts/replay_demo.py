"""Local quarantine replay evidence using real Delta reads and writes."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Row
from pyspark.sql.functions import col

from jobs._common import read_delta, spark_session, write_delta
from pipelines.bronze import annotate_bronze
from pipelines.config import load_settings
from pipelines.quality import QualityFramework, Rule
from pipelines.quality.replay import replay_quarantine
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA
from pipelines.silver.trades_pipeline import split_trades


def _replay_rules_without_symbol_allowlist() -> list[Rule]:
    return [rule for rule in TRADE_RULES if rule.error_code != "SYMBOL_UNKNOWN"]


def main() -> None:
    settings = load_settings()
    spark = spark_session("quarantine-replay-demo", settings)
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = settings.table_path("demo", "bronze_replay_trades")
    silver_path = settings.table_path("demo", "silver_trades")
    quarantine_path = settings.table_path("demo", "silver_quarantine_trades")

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

    before_silver = read_delta(spark, silver_path).count()
    before_quarantine = read_delta(spark, quarantine_path).count()

    now_passing, still_failing = replay_quarantine(
        read_delta(spark, quarantine_path),
        QualityFramework(_replay_rules_without_symbol_allowlist()),
    )
    write_delta(now_passing, silver_path, partition_by=["event_date"], mode="append")
    write_delta(still_failing, quarantine_path, partition_by=["quarantined_date"], mode="overwrite")

    after_silver = read_delta(spark, silver_path).count()
    after_quarantine = read_delta(spark, quarantine_path).count()
    replayed_ids = [
        row.trade_id
        for row in read_delta(spark, silver_path)
        .filter(col("trade_id") == "binance:9001")
        .select("trade_id")
        .collect()
    ]

    print("=== REPLAY DEMO ===")
    print(f"before: silver={before_silver}, quarantine={before_quarantine}")
    print(f"after: silver={after_silver}, quarantine={after_quarantine}")
    print(f"replayed_trade_ids={replayed_ids}")

    if before_silver != 0 or before_quarantine != 1 or after_silver != 1 or after_quarantine != 0:
        raise RuntimeError("Replay demo did not move exactly one quarantined row into Silver")

    spark.stop()


if __name__ == "__main__":
    main()

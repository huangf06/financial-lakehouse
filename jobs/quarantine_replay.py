"""Spark job: replay Silver trade quarantine records."""

from __future__ import annotations

from jobs._common import read_delta, spark_session, write_delta
from pipelines.config import load_settings
from pipelines.quality import QualityFramework
from pipelines.quality.replay import replay_quarantine
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES


def main() -> None:
    settings = load_settings()
    spark = spark_session("quarantine-replay", settings)
    quarantine_path = settings.table_path("silver", "quarantine_trades")
    target_path = settings.table_path("silver", "trades")
    now_passing, still_failing = replay_quarantine(
        read_delta(spark, quarantine_path),
        QualityFramework(TRADE_RULES),
    )
    write_delta(now_passing, target_path, ["event_date"])
    write_delta(still_failing, quarantine_path, ["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

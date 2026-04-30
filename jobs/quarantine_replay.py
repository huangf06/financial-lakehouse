"""Spark job: replay Silver trade quarantine records."""

from __future__ import annotations

import os

from jobs._common import read_delta, spark_session, write_delta
from pipelines.quality import QualityFramework
from pipelines.quality.replay import replay_quarantine
from pipelines.quality.rules.bronze_to_silver_trades import TRADE_RULES


def main() -> None:
    spark = spark_session("quarantine-replay")
    quarantine_path = os.environ["SILVER_QUARANTINE_TRADES_PATH"]
    target_path = os.environ["SILVER_TRADES_PATH"]
    now_passing, still_failing = replay_quarantine(
        read_delta(spark, quarantine_path),
        QualityFramework(TRADE_RULES),
    )
    write_delta(now_passing, target_path, ["event_date"])
    write_delta(still_failing, quarantine_path, ["quarantined_date"])
    spark.stop()


if __name__ == "__main__":
    main()

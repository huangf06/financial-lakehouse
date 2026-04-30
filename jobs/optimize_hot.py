"""Spark job: compact recent hot partitions."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.maintenance.optimize import run_optimize


def main() -> None:
    spark = spark_session("optimize-hot")
    run_optimize(
        spark,
        os.environ.get("OPTIMIZE_TABLE", "silver.trades"),
        os.environ.get("OPTIMIZE_LAYER", "silver"),
        os.environ.get("OPTIMIZE_NAME", "trades"),
        os.environ.get("OPTIMIZE_WHERE", "event_date >= current_date() - INTERVAL 2 DAYS"),
    )
    spark.stop()


if __name__ == "__main__":
    main()

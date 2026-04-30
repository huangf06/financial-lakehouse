"""Spark job: full Z-order optimize."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.maintenance.optimize import run_optimize


def main() -> None:
    spark = spark_session("optimize-zorder")
    run_optimize(
        spark,
        os.environ.get("OPTIMIZE_TABLE", "silver.trades"),
        os.environ.get("OPTIMIZE_LAYER", "silver"),
        os.environ.get("OPTIMIZE_NAME", "trades"),
    )
    spark.stop()


if __name__ == "__main__":
    main()

"""Spark job: Delta VACUUM."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.maintenance.vacuum import run_vacuum


def main() -> None:
    spark = spark_session("vacuum")
    run_vacuum(
        spark,
        os.environ.get("VACUUM_TABLE", "silver.trades"),
        int(os.environ.get("VACUUM_RETAIN_HOURS", "168")),
    )
    spark.stop()


if __name__ == "__main__":
    main()

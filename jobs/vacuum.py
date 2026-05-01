"""Spark job: Delta VACUUM."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.config import load_settings
from pipelines.maintenance.vacuum import delta_path_identifier, run_vacuum


def main() -> None:
    settings = load_settings()
    spark = spark_session("vacuum", settings)
    layer = os.environ.get("VACUUM_LAYER", "silver")
    table = os.environ.get("VACUUM_NAME", "trades")
    run_vacuum(
        spark,
        os.environ.get("VACUUM_TABLE", delta_path_identifier(settings.table_path(layer, table))),
        int(os.environ.get("VACUUM_RETAIN_HOURS", "168")),
    )
    spark.stop()


if __name__ == "__main__":
    main()

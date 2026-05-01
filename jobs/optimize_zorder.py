"""Spark job: full Z-order optimize."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.config import load_settings
from pipelines.maintenance.optimize import delta_path_identifier, run_optimize


def main() -> None:
    settings = load_settings()
    spark = spark_session("optimize-zorder", settings)
    layer = os.environ.get("OPTIMIZE_LAYER", "silver")
    table = os.environ.get("OPTIMIZE_NAME", "trades")
    run_optimize(
        spark,
        os.environ.get("OPTIMIZE_TABLE", delta_path_identifier(settings.table_path(layer, table))),
        layer,
        table,
    )
    spark.stop()


if __name__ == "__main__":
    main()

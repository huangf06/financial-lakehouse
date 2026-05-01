"""Spark job: Alpaca landing JSONL to Bronze Delta."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.config import load_settings
from pipelines.schemas.bronze import ALPACA_BAR_SCHEMA


def main() -> None:
    settings = load_settings()
    spark = spark_session("bronze-alpaca-bars", settings)

    stream = bronze_stream_reader(
        spark,
        source="alpaca",
        landing_path=settings.landing_path("alpaca"),
        schema_path=settings.schema_path("alpaca"),
        initial_schema=ALPACA_BAR_SCHEMA,
    )
    query = write_bronze_stream(
        stream,
        source="alpaca",
        table_path=settings.table_path("bronze", "alpaca_bars"),
        checkpoint_location=settings.checkpoint("bronze_alpaca_bars"),
        trigger_seconds=int(os.environ.get("BRONZE_TRIGGER_SECONDS", "30")),
        available_now=os.environ.get("BRONZE_TRIGGER_AVAILABLE_NOW", "false").lower() == "true",
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()

"""Spark job: Binance landing JSONL to Bronze Delta."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.config import load_settings
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA


def main() -> None:
    settings = load_settings()
    spark = spark_session("bronze-binance-stream", settings)

    stream = bronze_stream_reader(
        spark,
        source="binance",
        landing_path=settings.landing_path("binance"),
        schema_path=settings.schema_path("binance"),
        initial_schema=BINANCE_TRADE_SCHEMA,
    )
    query = write_bronze_stream(
        stream,
        source="binance",
        table_path=settings.table_path("bronze", "binance_trades"),
        checkpoint_location=settings.checkpoint("bronze_binance_trades"),
        trigger_seconds=int(os.environ.get("BRONZE_TRIGGER_SECONDS", "30")),
        available_now=os.environ.get("BRONZE_TRIGGER_AVAILABLE_NOW", "false").lower() == "true",
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()

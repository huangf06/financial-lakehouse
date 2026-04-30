"""Spark job: Binance landing JSONL to Bronze Delta."""

from __future__ import annotations

import os

from jobs._common import spark_session
from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA


def main() -> None:
    spark = spark_session("bronze-binance-stream")
    lakehouse_root = os.environ.get("LAKEHOUSE_ROOT", "s3a://lakehouse")
    checkpoint_root = os.environ.get("CHECKPOINT_ROOT", "s3a://lakehouse-meta/_checkpoints")

    stream = bronze_stream_reader(
        spark,
        source="binance",
        landing_path=os.environ.get("BINANCE_LANDING_PATH", f"{lakehouse_root}/landing/binance"),
        schema_path=os.environ.get("BINANCE_SCHEMA_PATH", f"{lakehouse_root}/_schemas/binance"),
        initial_schema=BINANCE_TRADE_SCHEMA,
    )
    query = write_bronze_stream(
        stream,
        source="binance",
        table_path=os.environ.get(
            "BRONZE_BINANCE_TRADES_PATH", f"{lakehouse_root}/bronze/binance_trades"
        ),
        checkpoint_location=os.environ.get(
            "BRONZE_BINANCE_CHECKPOINT",
            f"{checkpoint_root}/bronze_binance_trades",
        ),
        trigger_seconds=int(os.environ.get("BRONZE_TRIGGER_SECONDS", "30")),
        available_now=os.environ.get("BRONZE_TRIGGER_AVAILABLE_NOW", "false").lower() == "true",
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()

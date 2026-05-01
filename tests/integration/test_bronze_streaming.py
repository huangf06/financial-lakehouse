"""Bronze streaming integration evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA


@pytest.mark.integration
def test_bronze_available_now_writes_annotated_delta(
    spark: SparkSession, landing_path: str, table_path: str, checkpoint_path: str
) -> None:
    landing = Path(landing_path) / "binance"
    batch_dir = landing / "2026-04-30" / "12" / "00"
    batch_dir.mkdir(parents=True, exist_ok=True)
    (batch_dir / "batch.jsonl").write_text(
        json.dumps(
            {
                "event_type": "trade",
                "event_time": "2026-04-30T12:00:00Z",
                "symbol": "BTCUSDT",
                "trade_id": 42,
                "price": "64000.00",
                "quantity": "0.10",
                "trade_time": "2026-04-30T12:00:00Z",
                "buyer_is_maker": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    stream = bronze_stream_reader(
        spark,
        source="binance",
        landing_path=str(landing),
        schema_path=str(Path(landing_path) / "_schemas" / "binance"),
        initial_schema=BINANCE_TRADE_SCHEMA,
    )
    query = write_bronze_stream(
        stream,
        source="binance",
        table_path=table_path,
        checkpoint_location=checkpoint_path,
        available_now=True,
    )
    query.awaitTermination()

    result = spark.read.format("delta").load(table_path)
    row = result.select("trade_id", "_source", "_raw_json", "_file_path").collect()[0]
    assert result.count() == 1
    assert row.trade_id == 42
    assert row._source == "binance"
    assert row._raw_json is not None
    assert row._file_path.endswith("batch.jsonl")

"""Checkpoint recovery evidence for the production Bronze stream path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _run_bronze_once(
    spark: SparkSession,
    landing: Path,
    table_path: str,
    checkpoint_path: str,
    schema_path: str,
) -> None:
    stream = bronze_stream_reader(
        spark,
        source="binance",
        landing_path=str(landing),
        schema_path=schema_path,
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


@pytest.mark.integration
def test_restart_resumes_from_checkpoint(
    spark: SparkSession, landing_path: str, table_path: str, checkpoint_path: str
) -> None:
    landing = Path(landing_path) / "binance"
    schema_path = str(Path(landing_path) / "_schemas" / "binance")

    _write_jsonl(
        landing / "2026-04-30" / "10" / "00" / "batch1.jsonl",
        [
            {
                "event_type": "trade",
                "event_time": "2026-04-30T10:00:00Z",
                "symbol": "BTCUSDT",
                "trade_id": 1,
                "price": "100",
                "quantity": "0.01",
                "trade_time": "2026-04-30T10:00:00Z",
                "buyer_is_maker": False,
            }
        ],
    )
    _run_bronze_once(spark, landing, table_path, checkpoint_path, schema_path)
    assert spark.read.format("delta").load(table_path).count() == 1

    _write_jsonl(
        landing / "2026-04-30" / "10" / "01" / "batch2.jsonl",
        [
            {
                "event_type": "trade",
                "event_time": "2026-04-30T10:01:00Z",
                "symbol": "BTCUSDT",
                "trade_id": 2,
                "price": "101",
                "quantity": "0.02",
                "trade_time": "2026-04-30T10:01:00Z",
                "buyer_is_maker": True,
            }
        ],
    )
    _run_bronze_once(spark, landing, table_path, checkpoint_path, schema_path)

    final = spark.read.format("delta").load(table_path)
    assert final.count() == 2
    assert sorted(row.trade_id for row in final.select("trade_id").collect()) == [1, 2]

"""Additive schema tolerance evidence for the production Bronze stream path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.bronze import bronze_stream_reader, write_bronze_stream
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA


@pytest.mark.integration
def test_oss_bronze_additive_fields_do_not_break_ingestion(
    spark: SparkSession, landing_path: str, table_path: str, checkpoint_path: str
) -> None:
    """OSS Spark Bronze uses a fixed schema, so unknown additive fields are dropped.

    Databricks Auto Loader is the branch that evolves schemas with
    `cloudFiles.schemaEvolutionMode`. The local OSS contract is narrower:
    additive producer fields must not fail ingestion or lose known fields.
    """
    landing = Path(landing_path) / "binance"
    schema_path = str(Path(landing_path) / "_schemas" / "binance")
    (landing / "2026-04-30" / "10" / "00").mkdir(parents=True, exist_ok=True)
    (landing / "2026-04-30" / "10" / "01").mkdir(parents=True, exist_ok=True)

    (landing / "2026-04-30" / "10" / "00" / "v1.jsonl").write_text(
        json.dumps(
            {
                "event_type": "trade",
                "event_time": "2026-04-30T10:00:00+00:00",
                "symbol": "BTCUSDT",
                "trade_id": 100,
                "price": "65000",
                "quantity": "0.01",
                "trade_time": "2026-04-30T10:00:00Z",
                "buyer_is_maker": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (landing / "2026-04-30" / "10" / "01" / "v2.jsonl").write_text(
        json.dumps(
            {
                "event_type": "trade",
                "event_time": "2026-04-30T10:01:00+00:00",
                "symbol": "BTCUSDT",
                "trade_id": 101,
                "price": "65010",
                "quantity": "0.02",
                "trade_time": "2026-04-30T10:01:00Z",
                "buyer_is_maker": True,
                "is_self_match": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

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

    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2
    assert "is_self_match" not in result.columns
    assert sorted(row.trade_id for row in result.select("trade_id").collect()) == [100, 101]
    assert result.filter("trade_id = 101").select("symbol").collect()[0].symbol == "BTCUSDT"

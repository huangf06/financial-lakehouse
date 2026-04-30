"""Schema evolution evidence for local Spark JSON file streams."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.mark.integration
def test_schema_evolution_v1_to_v2_zero_loss(
    spark: SparkSession, landing_path: str, table_path: str, checkpoint_path: str
) -> None:
    """Process v1 and v2 producer files in one available-now stream.

    Databricks Auto Loader handles this with `cloudFiles.schemaEvolutionMode`.
    The local OSS Spark evidence uses file-stream schema inference over the
    available input set to verify the same downstream contract: no record loss,
    the new column is present, and old rows get nulls for the new field.
    """
    previous = spark.conf.get("spark.sql.streaming.schemaInference", "false")
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    landing = Path(landing_path) / "binance"
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
                "is_self_match": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    try:
        query = (
            spark.readStream.option("recursiveFileLookup", "true")
            .json(str(landing))
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start(table_path)
        )
        query.awaitTermination()

        result = spark.read.parquet(table_path)
        assert result.count() == 2
        assert "is_self_match" in result.columns
        assert result.filter("trade_id = 101").collect()[0]["is_self_match"] is True
        assert result.filter("trade_id = 100").collect()[0]["is_self_match"] is None
    finally:
        spark.conf.set("spark.sql.streaming.schemaInference", previous)

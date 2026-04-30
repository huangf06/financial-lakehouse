"""Checkpoint recovery evidence for Spark file streams."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


@pytest.mark.integration
def test_restart_resumes_from_checkpoint(
    spark: SparkSession, landing_path: str, table_path: str, checkpoint_path: str
) -> None:
    previous = spark.conf.get("spark.sql.streaming.schemaInference", "false")
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    landing = Path(landing_path) / "binance"

    try:
        _write_jsonl(
            landing / "2026-04-30" / "10" / "00" / "batch1.jsonl",
            [{"event_type": "trade", "symbol": "BTCUSDT", "trade_id": 1, "price": "100"}],
        )
        query = (
            spark.readStream.option("recursiveFileLookup", "true")
            .json(str(landing))
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start(table_path)
        )
        query.awaitTermination()
        assert spark.read.parquet(table_path).count() == 1

        _write_jsonl(
            landing / "2026-04-30" / "10" / "01" / "batch2.jsonl",
            [{"event_type": "trade", "symbol": "BTCUSDT", "trade_id": 2, "price": "101"}],
        )
        restarted = (
            spark.readStream.option("recursiveFileLookup", "true")
            .json(str(landing))
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start(table_path)
        )
        restarted.awaitTermination()

        final = spark.read.parquet(table_path)
        assert final.count() == 2
        assert sorted(row.trade_id for row in final.select("trade_id").collect()) == [1, 2]
    finally:
        spark.conf.set("spark.sql.streaming.schemaInference", previous)

"""End-to-end test: ReplayProducer Parquet -> JSONL -> Bronze Delta on local fs.

Mirrors the compose-level demo without requiring MinIO; uses a temp dir as the
landing root and writes Bronze to a local Delta path.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from pipelines.bronze import annotate_bronze
from pipelines.schemas.bronze import BINANCE_TRADE_SCHEMA
from producers.replay import ReplayProducer


def _make_parquet(path: Path, rows: int) -> None:
    base = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    records = [
        {
            "event_type": "trade",
            "event_time": base + timedelta(seconds=2 * i),
            "symbol": "BTCUSDT",
            "trade_id": 10_000 + i,
            "price": f"{65000.0 + i:.2f}",
            "quantity": "0.010000",
            "trade_time": base + timedelta(seconds=2 * i),
            "buyer_is_maker": bool(i % 2),
        }
        for i in range(rows)
    ]
    pd.DataFrame(records).to_parquet(path, index=False)


@pytest.mark.integration
def test_replay_parquet_lands_in_bronze_delta(spark: SparkSession, tmp_path: Path) -> None:
    parquet = tmp_path / "binance.parquet"
    landing_root = tmp_path / "landing"
    bronze_path = tmp_path / "bronze" / "binance_replay_trades"
    _make_parquet(parquet, rows=20)

    asyncio.run(
        ReplayProducer(
            source="replay/binance",
            parquet_path=parquet,
            landing_root=landing_root,
            speedup=10_000.0,
            timestamp_col="event_time",
        ).run()
    )

    jsonl_files = list((landing_root / "replay" / "binance").rglob("*.jsonl"))
    assert jsonl_files, "ReplayProducer must emit at least one JSONL file"

    df = (
        spark.read.option("recursiveFileLookup", "true")
        .schema(BINANCE_TRADE_SCHEMA)
        .json(str(landing_root / "replay" / "binance"))
    )
    df = annotate_bronze(df, source="replay_binance")
    df.write.format("delta").mode("overwrite").save(str(bronze_path))

    delta_count = spark.read.format("delta").load(str(bronze_path)).count()
    assert delta_count == 20

    df2 = (
        spark.read.option("recursiveFileLookup", "true")
        .schema(BINANCE_TRADE_SCHEMA)
        .json(str(landing_root / "replay" / "binance"))
    )
    df2 = annotate_bronze(df2, source="replay_binance")
    df2.write.format("delta").mode("overwrite").save(str(bronze_path))
    assert spark.read.format("delta").load(str(bronze_path)).count() == 20

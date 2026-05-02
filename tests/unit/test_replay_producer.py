"""Tests for deterministic historical Parquet replay."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from producers.replay import ReplayProducer


@pytest.mark.asyncio
async def test_replay_producer_writes_time_ordered_jsonl(tmp_path: Path) -> None:
    parquet_path = tmp_path / "history.parquet"
    landing_root = tmp_path / "landing"
    table = pa.table(
        {
            "event_time": [
                datetime(2026, 5, 2, 12, 2, tzinfo=UTC),
                datetime(2026, 5, 2, 12, 0, tzinfo=UTC),
                datetime(2026, 5, 2, 12, 1, tzinfo=UTC),
            ],
            "symbol": ["MSFT", "AAPL", "NVDA"],
            "price": [410.25, 175.5, 880.0],
        }
    )
    pq.write_table(table, parquet_path)

    producer = ReplayProducer(
        source="history",
        parquet_path=parquet_path,
        landing_root=landing_root,
        speedup=0,
    )
    await producer.run()

    files = list(landing_root.rglob("*.jsonl"))
    assert len(files) == 1
    rows = [json.loads(line) for line in files[0].read_text().splitlines()]
    assert [row["symbol"] for row in rows] == ["AAPL", "NVDA", "MSFT"]
    assert [row["event_time"] for row in rows] == [
        "2026-05-02T12:00:00+00:00",
        "2026-05-02T12:01:00+00:00",
        "2026-05-02T12:02:00+00:00",
    ]
    assert list(landing_root.rglob(".tmp.*")) == []

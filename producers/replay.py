"""Deterministic replay of historical Parquet data to JSONL landing files."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from producers.base import AtomicJsonlWriter


class ReplayProducer:
    def __init__(
        self,
        source: str,
        parquet_path: Path,
        landing_root: Path,
        speedup: float = 1.0,
        timestamp_col: str = "event_time",
    ) -> None:
        self._source = source
        self._parquet_path = Path(parquet_path)
        self._timestamp_col = timestamp_col
        self._speedup = speedup
        self._writer = AtomicJsonlWriter(landing_root=landing_root, source=source)

    @staticmethod
    def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}

    async def run(self) -> None:
        rows = pq.read_table(self._parquet_path).to_pylist()
        rows.sort(key=lambda r: r[self._timestamp_col])
        previous_ts: datetime | None = None
        async with self._writer:
            for row in rows:
                row_ts = row[self._timestamp_col]
                if isinstance(row_ts, str):
                    row_ts = datetime.fromisoformat(row_ts.replace("Z", "+00:00"))
                if previous_ts is not None and self._speedup > 0:
                    await asyncio.sleep(
                        max(0.0, (row_ts - previous_ts).total_seconds() / self._speedup)
                    )
                await self._writer.write(self._row_to_dict(row))
                previous_ts = row_ts

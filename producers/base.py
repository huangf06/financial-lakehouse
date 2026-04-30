"""Base producer with atomic JSONL file landing."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Self


class AtomicJsonlWriter:
    """Async JSONL writer that rotates files and renames `.tmp.*` to final atomically."""

    def __init__(
        self,
        landing_root: Path,
        source: str,
        rotation_seconds: float = 10.0,
        rotation_records: int = 1000,
    ) -> None:
        self._root = Path(landing_root)
        self._source = source
        self._rotation_seconds = rotation_seconds
        self._rotation_records = rotation_records
        self._current_path: Path | None = None
        self._current_tmp: Path | None = None
        self._current_records = 0
        self._current_started_at = 0.0
        self._fh: Any = None
        self._shard = uuid.uuid4().hex[:6]
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.flush()

    def _build_path(self, ts: datetime) -> tuple[Path, Path]:
        directory = self._root / self._source / f"{ts:%Y-%m-%d}" / f"{ts:%H}" / f"{ts:%M}"
        directory.mkdir(parents=True, exist_ok=True)
        final = directory / f"{self._shard}-{int(ts.timestamp() * 1000)}.jsonl"
        return final, directory / f".tmp.{final.name}"

    async def _open_new(self) -> None:
        self._current_path, self._current_tmp = self._build_path(datetime.now(tz=UTC))
        self._fh = self._current_tmp.open("w", encoding="utf-8")  # noqa: SIM115
        self._current_records = 0
        self._current_started_at = time.monotonic()

    async def _close_current(self) -> None:
        if self._fh is None or self._current_tmp is None or self._current_path is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        if self._current_records > 0:
            self._current_tmp.rename(self._current_path)
        else:
            self._current_tmp.unlink(missing_ok=True)
        self._fh = None
        self._current_tmp = None
        self._current_path = None

    def _should_rotate(self) -> bool:
        return (
            self._fh is None
            or self._current_records >= self._rotation_records
            or (time.monotonic() - self._current_started_at) >= self._rotation_seconds
        )

    async def write(self, record: dict[str, Any]) -> None:
        async with self._lock:
            if self._should_rotate():
                await self._close_current()
                await self._open_new()
            assert self._fh is not None
            self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._current_records += 1

    async def flush(self) -> None:
        async with self._lock:
            await self._close_current()

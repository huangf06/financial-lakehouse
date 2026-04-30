"""Tests for producer base class file rotation and atomic write."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from producers.base import AtomicJsonlWriter


@pytest.mark.asyncio
async def test_atomic_writer_creates_file_with_records(tmp_path: Path) -> None:
    writer = AtomicJsonlWriter(tmp_path, "test", rotation_seconds=60.0, rotation_records=10)
    async with writer:
        await writer.write({"trade_id": 1})
        await writer.write({"trade_id": 2})
        await writer.flush()
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
    rows = [json.loads(line) for line in files[0].read_text().splitlines()]
    assert [row["trade_id"] for row in rows] == [1, 2]


@pytest.mark.asyncio
async def test_atomic_writer_rotates_on_count(tmp_path: Path) -> None:
    writer = AtomicJsonlWriter(tmp_path, "test", rotation_seconds=60.0, rotation_records=3)
    async with writer:
        for i in range(7):
            await writer.write({"trade_id": i})
        await writer.flush()
    assert len(list(tmp_path.rglob("*.jsonl"))) >= 3
    assert list(tmp_path.rglob(".tmp.*")) == []

"""Tests for producer base class file rotation and atomic write."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from producers.base import AtomicJsonlWriter, S3JsonlWriter, landing_writer_from_env


class FakeS3Client:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> None:
        self.puts.append(kwargs)


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


@pytest.mark.asyncio
async def test_s3_writer_publishes_complete_jsonl_objects() -> None:
    client = FakeS3Client()
    writer = S3JsonlWriter(
        "s3a://lakehouse/landing",
        "binance",
        rotation_seconds=60.0,
        rotation_records=2,
        s3_client=client,
    )

    async with writer:
        await writer.write({"trade_id": 1})
        await writer.write({"trade_id": 2})
        await writer.write({"trade_id": 3})

    assert len(client.puts) == 2
    assert {put["Bucket"] for put in client.puts} == {"lakehouse"}
    assert all(put["Key"].startswith("landing/binance/") for put in client.puts)
    bodies = [put["Body"].decode() for put in client.puts]
    assert bodies == [
        '{"trade_id":1}\n{"trade_id":2}\n',
        '{"trade_id":3}\n',
    ]


def test_landing_writer_factory_keeps_local_path_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LANDING_ROOT", str(tmp_path))

    writer = landing_writer_from_env("binance")

    assert isinstance(writer, AtomicJsonlWriter)

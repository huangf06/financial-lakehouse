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
from typing import Any, Protocol, Self
from urllib.parse import urlparse

import boto3


class JsonlWriter(Protocol):
    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def write(self, record: dict[str, Any]) -> None: ...

    async def flush(self) -> None: ...


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


class S3JsonlWriter:
    """JSONL writer that publishes complete rotated objects to S3-compatible storage."""

    def __init__(
        self,
        landing_root: str,
        source: str,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region_name: str = "us-east-1",
        rotation_seconds: float = 10.0,
        rotation_records: int = 1000,
        s3_client: Any | None = None,
    ) -> None:
        parsed = urlparse(landing_root)
        if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
            raise ValueError(f"Expected s3/s3a landing root, got {landing_root!r}")
        self._bucket = parsed.netloc
        self._prefix = parsed.path.strip("/")
        self._source = source
        self._rotation_seconds = rotation_seconds
        self._rotation_records = rotation_records
        self._current_records = 0
        self._current_started_at = 0.0
        self._buffer: list[str] = []
        self._current_key: str | None = None
        self._shard = uuid.uuid4().hex[:6]
        self._lock = asyncio.Lock()
        self._s3 = (
            s3_client
            if s3_client is not None
            else boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
            )
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.flush()

    def _build_key(self, ts: datetime) -> str:
        prefix = f"{self._prefix}/" if self._prefix else ""
        return (
            f"{prefix}{self._source}/{ts:%Y-%m-%d/%H/%M}/"
            f"{self._shard}-{int(ts.timestamp() * 1000)}.jsonl"
        )

    def _open_new(self) -> None:
        self._current_key = self._build_key(datetime.now(tz=UTC))
        self._buffer = []
        self._current_records = 0
        self._current_started_at = time.monotonic()

    async def _close_current(self) -> None:
        if self._current_key is None:
            return
        if self._current_records > 0:
            body = ("\n".join(self._buffer) + "\n").encode("utf-8")
            await asyncio.to_thread(
                self._s3.put_object,
                Bucket=self._bucket,
                Key=self._current_key,
                Body=body,
            )
        self._current_key = None
        self._buffer = []
        self._current_records = 0

    def _should_rotate(self) -> bool:
        return (
            self._current_key is None
            or self._current_records >= self._rotation_records
            or (time.monotonic() - self._current_started_at) >= self._rotation_seconds
        )

    async def write(self, record: dict[str, Any]) -> None:
        async with self._lock:
            if self._should_rotate():
                await self._close_current()
                self._open_new()
            self._buffer.append(json.dumps(record, separators=(",", ":")))
            self._current_records += 1

    async def flush(self) -> None:
        async with self._lock:
            await self._close_current()


def landing_writer_from_env(source: str) -> JsonlWriter:
    landing_root = os.environ.get("LANDING_ROOT")
    if landing_root is None:
        lakehouse_root = os.environ.get("LAKEHOUSE_ROOT", "s3a://lakehouse").rstrip("/")
        landing_root = f"{lakehouse_root}/landing"
    if landing_root.startswith(("s3://", "s3a://")):
        return S3JsonlWriter(
            landing_root=landing_root,
            source=source,
            endpoint_url=os.environ.get("S3_ENDPOINT"),
            access_key=os.environ.get("S3_ACCESS_KEY"),
            secret_key=os.environ.get("S3_SECRET_KEY"),
            region_name=os.environ.get("S3_REGION", "us-east-1"),
        )
    return AtomicJsonlWriter(landing_root=Path(landing_root), source=source)

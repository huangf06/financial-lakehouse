"""Generate synthetic records into MinIO landing/binance for smoke tests."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import boto3

from pipelines.config import load_settings


def _bucket_and_prefix(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Expected s3/s3a path, got {path!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def main() -> None:
    settings = load_settings()
    storage = settings.storage
    s3 = boto3.client(
        "s3",
        endpoint_url=storage.endpoint,
        aws_access_key_id=storage.access_key,
        aws_secret_access_key=storage.secret_key,
        region_name=storage.region,
    )
    now = datetime.now(tz=UTC)
    lines = []
    for i in range(50):
        ts = now - timedelta(seconds=50 - i)
        lines.append(
            json.dumps(
                {
                    "event_type": "trade",
                    "event_time": ts.isoformat(),
                    "trade_time": ts.isoformat(),
                    "symbol": random.choice(["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
                    "trade_id": 1_000_000 + i,
                    "price": f"{random.uniform(50000, 70000):.2f}",
                    "quantity": f"{random.uniform(0.001, 0.5):.6f}",
                    "buyer_is_maker": bool(random.getrandbits(1)),
                }
            )
        )
    bucket, prefix = _bucket_and_prefix(settings.landing_path("binance"))
    key = f"{prefix}/{now:%Y-%m-%d/%H/%M}/seed-{int(now.timestamp())}.jsonl"
    s3.put_object(Bucket=bucket, Key=key, Body=("\n".join(lines) + "\n").encode())
    print(f"Seeded s3://{bucket}/{key} with 50 records")


if __name__ == "__main__":
    main()

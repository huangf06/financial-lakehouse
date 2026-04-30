"""Generate synthetic records into MinIO landing/binance for smoke tests."""

from __future__ import annotations

import json
import os
import random
from datetime import UTC, datetime, timedelta

import boto3


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
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
    key = f"landing/binance/{now:%Y-%m-%d/%H/%M}/seed-{int(now.timestamp())}.jsonl"
    s3.put_object(Bucket="lakehouse", Key=key, Body=("\n".join(lines) + "\n").encode())
    print(f"Seeded s3://lakehouse/{key} with 50 records")


if __name__ == "__main__":
    main()

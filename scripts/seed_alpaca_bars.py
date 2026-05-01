"""Generate synthetic Alpaca bar records into MinIO landing/alpaca."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import boto3

from pipelines.config import load_settings
from scripts.seed_local_data import _bucket_and_prefix


def synthetic_alpaca_bars(bar_start: datetime) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        {
            "symbol": "AAPL",
            "bar_open_ts": (bar_start + timedelta(minutes=i)).isoformat(),
            "timeframe": "1m",
            "open": 100.0 + i,
            "high": 102.0 + i,
            "low": 99.0 + i,
            "close": 101.0 + i,
            "volume": 1000 + i,
            "vwap": 100.5 + i,
            "trade_count": 10 + i,
        }
        for i in range(5)
    ]
    rows.append(
        {
            "symbol": "AAPL",
            "bar_open_ts": (bar_start + timedelta(minutes=5)).isoformat(),
            "timeframe": "1m",
            "open": 105.0,
            "high": 106.0,
            "low": 104.0,
            "close": 105.5,
            "volume": -1,
            "vwap": 105.25,
            "trade_count": 10,
        }
    )
    return rows


def alpaca_bars_landing_key(prefix: str, now: datetime) -> str:
    return f"{prefix}/{now:%Y-%m-%d/%H/%M}/bars-{int(now.timestamp())}.jsonl"


def jsonl_body(rows: list[dict[str, object]]) -> bytes:
    return ("\n".join(json.dumps(row) for row in rows) + "\n").encode()


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
    bar_start = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
    rows = synthetic_alpaca_bars(bar_start)

    bucket, prefix = _bucket_and_prefix(settings.landing_path("alpaca"))
    key = alpaca_bars_landing_key(prefix, now)
    s3.put_object(Bucket=bucket, Key=key, Body=jsonl_body(rows))
    print(f"Seeded s3://{bucket}/{key} with {len(rows)} Alpaca bars")


if __name__ == "__main__":
    main()

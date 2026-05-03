"""CLI wrapper for Parquet replay."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from producers.replay import ReplayProducer


def _landing_root(value: str) -> str | Path:
    if value.startswith(("s3://", "s3a://")):
        return value
    return Path(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--parquet", required=True, type=Path)
    parser.add_argument("--landing-root", required=True, type=_landing_root)
    parser.add_argument("--speedup", type=float, default=1.0)
    parser.add_argument("--timestamp-col", default="event_time")
    args = parser.parse_args()
    asyncio.run(
        ReplayProducer(
            source=args.source,
            parquet_path=args.parquet,
            landing_root=args.landing_root,
            speedup=args.speedup,
            timestamp_col=args.timestamp_col,
        ).run()
    )


if __name__ == "__main__":
    main()

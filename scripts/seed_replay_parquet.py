"""Generate a deterministic Parquet of synthetic Binance trades for replay demo.

Output column names + types match BINANCE_TRADE_SCHEMA so ReplayProducer can
emit JSONL records that the Bronze stream reader fully populates.
"""

from __future__ import annotations

import argparse
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd


def _generate_rows(count: int, start: datetime) -> pd.DataFrame:
    rng = random.Random(42)
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    base_price = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "BNBUSDT": 600.0}
    rows = []
    for i in range(count):
        symbol = symbols[i % len(symbols)]
        event_time = start + timedelta(seconds=i * 2)
        price = base_price[symbol] + rng.uniform(-100, 100)
        quantity = round(rng.uniform(0.001, 1.5), 6)
        rows.append(
            {
                "event_type": "trade",
                "event_time": event_time,
                "symbol": symbol,
                "trade_id": 10_000_000 + i,
                "price": f"{price:.2f}",
                "quantity": f"{quantity:.6f}",
                "trade_time": event_time,
                "buyer_is_maker": bool(i % 2),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/replay/binance_2024-01-01.parquet"),
    )
    parser.add_argument("--rows", type=int, default=50)
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df = _generate_rows(args.rows, datetime(2024, 1, 1, 0, 0, tzinfo=UTC))
    df.to_parquet(args.out, index=False)
    print(f"wrote {len(df)} rows -> {args.out}")


if __name__ == "__main__":
    main()

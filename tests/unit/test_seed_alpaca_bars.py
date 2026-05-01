"""Tests for Alpaca bar seed helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from scripts.seed_alpaca_bars import alpaca_bars_landing_key, jsonl_body, synthetic_alpaca_bars


def test_synthetic_alpaca_bars_include_valid_and_invalid_rows() -> None:
    rows = synthetic_alpaca_bars(datetime(2026, 4, 30, 12, tzinfo=UTC))

    assert len(rows) == 6
    assert [row["bar_open_ts"] for row in rows] == [
        "2026-04-30T12:00:00+00:00",
        "2026-04-30T12:01:00+00:00",
        "2026-04-30T12:02:00+00:00",
        "2026-04-30T12:03:00+00:00",
        "2026-04-30T12:04:00+00:00",
        "2026-04-30T12:05:00+00:00",
    ]
    assert [row["volume"] for row in rows] == [1000, 1001, 1002, 1003, 1004, -1]
    assert {row["symbol"] for row in rows} == {"AAPL"}
    assert {row["timeframe"] for row in rows} == {"1m"}


def test_alpaca_bars_landing_key_uses_minute_partition() -> None:
    key = alpaca_bars_landing_key("landing/alpaca", datetime(2026, 5, 2, 8, 9, 10, tzinfo=UTC))

    assert key == "landing/alpaca/2026-05-02/08/09/bars-1777709350.jsonl"


def test_jsonl_body_round_trips_rows() -> None:
    rows = synthetic_alpaca_bars(datetime(2026, 4, 30, 12, tzinfo=UTC))
    body = jsonl_body(rows)

    decoded = [json.loads(line) for line in body.decode().splitlines()]
    assert decoded == rows
    assert body.endswith(b"\n")

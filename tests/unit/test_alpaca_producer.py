"""Tests for Alpaca normalization."""

from __future__ import annotations

from producers.alpaca import normalize_bar


def test_normalize_bar() -> None:
    result = normalize_bar(
        {"t": "2026-04-30T14:30:00Z", "S": "AAPL", "o": 175.0, "v": 10, "experimental": 42}, "1Min"
    )
    assert result["symbol"] == "AAPL"
    assert result["bar_open_ts"] == "2026-04-30T14:30:00+00:00"
    assert result["timeframe"] == "1Min"
    assert result["open"] == 175.0
    assert result["experimental"] == 42

"""Tests for Binance normalization."""

from __future__ import annotations

from producers.binance import normalize_trade_message


def test_normalize_known_fields() -> None:
    result = normalize_trade_message(
        {"e": "trade", "E": 1714000000000, "s": "BTCUSDT", "t": 1, "p": "1", "q": "2", "m": True}
    )
    assert result["event_type"] == "trade"
    assert result["symbol"] == "BTCUSDT"
    assert result["trade_id"] == 1
    assert result["buyer_is_maker"] is True
    assert result["event_time"].startswith("2024-")


def test_normalize_preserves_unknown_fields() -> None:
    assert normalize_trade_message({"e": "trade", "is_self_match": True})["is_self_match"] is True

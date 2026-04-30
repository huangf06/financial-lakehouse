"""Source abstractions consumed by Spark readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SourceName = Literal["binance", "alpaca", "yfinance", "replay"]


@dataclass(frozen=True)
class SourceSpec:
    name: SourceName
    asset_class: Literal["crypto", "stock"]
    landing_subdir: str
    bronze_table: str


SOURCES: dict[str, SourceSpec] = {
    "binance": SourceSpec("binance", "crypto", "binance", "binance_trades"),
    "alpaca": SourceSpec("alpaca", "stock", "alpaca", "alpaca_bars"),
    "yfinance": SourceSpec("yfinance", "stock", "yfinance", "yfinance_history"),
}

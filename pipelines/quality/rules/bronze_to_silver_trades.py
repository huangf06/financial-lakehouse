"""Bronze to Silver trade rules."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.functions import col, expr, lit

from pipelines.quality import Rule

KNOWN_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "NVDA",
    "SPY",
    "QQQ",
}


def _price_in_range() -> Column:
    is_crypto = col("symbol").rlike("USDT$")
    return (is_crypto & (col("price") >= 0.000001) & (col("price") <= 10_000_000)) | (
        ~is_crypto & (col("price") >= 0.01) & (col("price") <= 1_000_000)
    )


TRADE_RULES: list[Rule] = [
    Rule("BR-001", "PRICE_NOT_POSITIVE", "error", "price > 0", lambda df: col("price") > 0),
    Rule(
        "BR-002",
        "PRICE_OUT_OF_RANGE",
        "error",
        "price in sanity range",
        lambda df: _price_in_range(),
    ),
    Rule(
        "BR-003", "QUANTITY_NOT_POSITIVE", "error", "quantity > 0", lambda df: col("quantity") > 0
    ),
    Rule(
        "BR-004",
        "SYMBOL_UNKNOWN",
        "error",
        "symbol in allowlist",
        lambda df: col("symbol").isin(*KNOWN_SYMBOLS),
    ),
    Rule(
        "BR-005",
        "EVENT_TS_INVALID",
        "error",
        "event_ts not null, not >1min future, not before 2015",
        lambda df: col("event_ts").isNotNull()
        & (col("event_ts") <= expr("current_timestamp() + INTERVAL 1 MINUTE"))
        & (col("event_ts") >= lit("2015-01-01").cast("timestamp")),
    ),
    Rule(
        "BR-006",
        "TRADE_ID_MISSING",
        "error",
        "trade_id not null",
        lambda df: col("trade_id").isNotNull(),
    ),
    Rule(
        "BR-007",
        "LATE_ARRIVAL",
        "warning",
        "ingest_ts - event_ts < 5 minutes",
        lambda df: (col("ingest_ts").cast("long") - col("event_ts").cast("long")) < 300,
    ),
]

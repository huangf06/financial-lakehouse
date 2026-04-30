"""Bronze to Silver bar rules."""

from __future__ import annotations

from pyspark.sql.functions import col

from pipelines.quality import Rule

VALID_TIMEFRAMES = {"1Min", "5Min", "15Min", "1Hour", "1Day", "1m", "5m", "1h", "1d"}

BAR_RULES: list[Rule] = [
    Rule(
        "BAR-001",
        "BAR_TIMESTAMP_NULL",
        "error",
        "bar_open_ts not null",
        lambda df: col("bar_open_ts").isNotNull(),
    ),
    Rule(
        "BAR-002",
        "BAR_SYMBOL_NULL",
        "error",
        "symbol not null",
        lambda df: col("symbol").isNotNull(),
    ),
    Rule(
        "BAR-003",
        "BAR_TIMEFRAME_INVALID",
        "error",
        "timeframe known",
        lambda df: col("timeframe").isin(*VALID_TIMEFRAMES),
    ),
    Rule(
        "BAR-004",
        "BAR_HIGH_LOW_INVERTED",
        "error",
        "high >= low",
        lambda df: col("high") >= col("low"),
    ),
    Rule(
        "BAR-005",
        "BAR_OPEN_OUT_OF_RANGE",
        "error",
        "low <= open <= high",
        lambda df: (col("open") >= col("low")) & (col("open") <= col("high")),
    ),
    Rule(
        "BAR-006",
        "BAR_CLOSE_OUT_OF_RANGE",
        "error",
        "low <= close <= high",
        lambda df: (col("close") >= col("low")) & (col("close") <= col("high")),
    ),
    Rule("BAR-007", "BAR_VOLUME_NEGATIVE", "error", "volume >= 0", lambda df: col("volume") >= 0),
]

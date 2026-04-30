"""Silver to Gold bar rules."""

from __future__ import annotations

from pyspark.sql.functions import col

from pipelines.quality import Rule

GOLD_BAR_RULES: list[Rule] = [
    Rule(
        "GR-001",
        "BAR_INCONSISTENT",
        "error",
        "high >= max(open, close), low <= min(open, close), high >= low",
        lambda df: (col("high") >= col("low"))
        & (col("high") >= col("open"))
        & (col("high") >= col("close"))
        & (col("low") <= col("open"))
        & (col("low") <= col("close")),
    ),
    Rule("GR-002", "VOLUME_NEGATIVE", "error", "volume >= 0", lambda df: col("volume") >= 0),
]

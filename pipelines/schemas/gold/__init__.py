"""Gold layer schemas."""

from pipelines.schemas.gold.bars_aggregated import GOLD_BARS_AGGREGATED_SCHEMA
from pipelines.schemas.gold.daily_volume import GOLD_DAILY_VOLUME_SCHEMA
from pipelines.schemas.gold.market_quality import GOLD_MARKET_QUALITY_SCHEMA
from pipelines.schemas.gold.quarantine import GOLD_QUARANTINE_BARS_SCHEMA

__all__ = [
    "GOLD_BARS_AGGREGATED_SCHEMA",
    "GOLD_DAILY_VOLUME_SCHEMA",
    "GOLD_MARKET_QUALITY_SCHEMA",
    "GOLD_QUARANTINE_BARS_SCHEMA",
]

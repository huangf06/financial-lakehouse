"""Silver quarantine schemas."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.schemas.silver.bars import SILVER_BARS_SCHEMA
from pipelines.schemas.silver.trades import SILVER_TRADES_SCHEMA

_QUARANTINE_EXTRAS = [
    StructField("_error_code", StringType(), False),
    StructField("_error_msg", StringType(), True),
    StructField("_quarantined_ts", TimestampType(), False),
    StructField("quarantined_date", DateType(), False),
    StructField("_replay_attempts", IntegerType(), False),
    StructField("_raw_json", StringType(), True),
]


def _extend(base: StructType) -> StructType:
    return StructType([f for f in base.fields if f.name != "event_date"] + _QUARANTINE_EXTRAS)


SILVER_QUARANTINE_TRADES_SCHEMA = _extend(SILVER_TRADES_SCHEMA)
SILVER_QUARANTINE_BARS_SCHEMA = _extend(SILVER_BARS_SCHEMA)

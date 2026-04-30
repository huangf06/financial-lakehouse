"""Gold quarantine schemas."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.schemas.gold.bars_aggregated import GOLD_BARS_AGGREGATED_SCHEMA

_QUARANTINE_EXTRAS = [
    StructField("_error_code", StringType(), False),
    StructField("_error_msg", StringType(), True),
    StructField("_quarantined_ts", TimestampType(), False),
    StructField("quarantined_date", DateType(), False),
    StructField("_replay_attempts", IntegerType(), False),
    StructField("_raw_json", StringType(), True),
]


def _extend(base: StructType) -> StructType:
    return StructType([f for f in base.fields if f.name != "bar_date"] + _QUARANTINE_EXTRAS)


GOLD_QUARANTINE_BARS_SCHEMA = _extend(GOLD_BARS_AGGREGATED_SCHEMA)

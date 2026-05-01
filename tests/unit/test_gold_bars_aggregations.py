"""Gold bars aggregation tests."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import Row, SparkSession

from pipelines.gold.bars_aggregations import aggregate_bars


def test_aggregate_bars_rolls_five_one_minute_rows_to_one_5m_bar(
    spark: SparkSession,
) -> None:
    start = datetime(2026, 4, 30, 12, 0)
    bars = spark.createDataFrame(
        [
            Row(
                bar_open_ts=start + timedelta(minutes=i),
                symbol="AAPL",
                asset_class="stock",
                timeframe="1m",
                open=Decimal(str(100 + i)),
                high=Decimal(str(102 + i)),
                low=Decimal(str(99 + i)),
                close=Decimal(str(101 + i)),
                volume=Decimal(str(i + 1)),
                vwap=Decimal(str(100 + i)),
                trade_count=i + 1,
            )
            for i in range(5)
        ]
    )

    result = aggregate_bars(bars, "5m")

    row = result.collect()[0]
    assert result.count() == 1
    assert row.bar_open_ts == start.replace(tzinfo=None)
    assert row.timeframe == "5m"
    assert row.open == Decimal("100")
    assert row.high == Decimal("106")
    assert row.low == Decimal("99")
    assert row.close == Decimal("105")
    assert row.volume == Decimal("15")
    assert row.vwap == Decimal("102.666667")
    assert row.trade_count == 15


def test_aggregate_bars_rejects_unknown_timeframe(spark: SparkSession) -> None:
    bars = spark.createDataFrame(
        [
            Row(
                bar_open_ts=datetime(2026, 4, 30, 12),
                symbol="AAPL",
                asset_class="stock",
                timeframe="1m",
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1"),
                vwap=Decimal("100"),
                trade_count=1,
            )
        ]
    )

    try:
        aggregate_bars(bars, "15m")
    except ValueError as exc:
        assert str(exc) == "Unsupported bar timeframe: 15m"
    else:
        raise AssertionError("expected ValueError")

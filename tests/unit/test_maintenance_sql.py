"""Maintenance SQL helper tests."""

from __future__ import annotations

from pipelines.maintenance.optimize import delta_path_identifier as optimize_delta_path_identifier
from pipelines.maintenance.optimize import optimize_sql
from pipelines.maintenance.vacuum import delta_path_identifier as vacuum_delta_path_identifier
from pipelines.maintenance.vacuum import vacuum_sql


def test_optimize_sql_supports_delta_path_identifier() -> None:
    table = optimize_delta_path_identifier("s3a://lakehouse/silver/trades")

    assert table == "delta.`s3a://lakehouse/silver/trades`"
    assert (
        optimize_sql(table, "silver", "trades", "event_date >= current_date() - INTERVAL 2 DAYS")
        == "OPTIMIZE delta.`s3a://lakehouse/silver/trades` "
        "WHERE event_date >= current_date() - INTERVAL 2 DAYS ZORDER BY (symbol)"
    )


def test_vacuum_sql_supports_delta_path_identifier() -> None:
    table = vacuum_delta_path_identifier("s3a://lakehouse/silver/trades")

    assert table == "delta.`s3a://lakehouse/silver/trades`"
    assert vacuum_sql(table, 168) == "VACUUM delta.`s3a://lakehouse/silver/trades` RETAIN 168 HOURS"

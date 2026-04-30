"""Airflow Dataset definitions with import-safe fallback for unit tests."""

from __future__ import annotations

try:
    from airflow import Dataset
except Exception:  # pragma: no cover

    class Dataset(str):  # type: ignore[no-redef]
        pass


silver_trades_ds = Dataset("delta://lakehouse/silver/trades")
silver_bars_ds = Dataset("delta://lakehouse/silver/bars")
gold_bars_ds = Dataset("delta://lakehouse/gold/bars")

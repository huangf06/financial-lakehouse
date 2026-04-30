"""Airflow Dataset evidence placeholder."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_dataset_symbols_exist() -> None:
    from dags._common.datasets import silver_bars_ds, silver_trades_ds

    assert silver_trades_ds
    assert silver_bars_ds

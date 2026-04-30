"""Z-order optimization evidence placeholder."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_zorder_sql_contains_symbol() -> None:
    from pipelines.maintenance.optimize import optimize_sql

    assert "ZORDER BY (symbol)" in optimize_sql("silver.trades", "silver", "trades")

"""Z-order optimization helper evidence."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_run_optimize_uses_partition_spec_zorder_and_predicate() -> None:
    from pipelines.maintenance.optimize import run_optimize

    class RecordingSpark:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> None:
            self.statements.append(statement)

    spark = RecordingSpark()
    run_optimize(
        spark,  # type: ignore[arg-type]
        "silver.trades",
        "silver",
        "trades",
        "event_date >= DATE '2026-04-30'",
    )

    assert spark.statements == [
        "OPTIMIZE silver.trades WHERE event_date >= DATE '2026-04-30' ZORDER BY (symbol)"
    ]

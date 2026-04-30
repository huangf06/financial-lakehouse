"""Spark job entrypoint import tests."""

from __future__ import annotations

import importlib


def test_job_entrypoints_import() -> None:
    for module in [
        "jobs.bronze_binance_stream",
        "jobs.silver_trades",
        "jobs.silver_bars",
        "jobs.gold_bars",
        "jobs.daily_volume",
        "jobs.market_quality",
        "jobs.quarantine_replay",
        "jobs.optimize_hot",
        "jobs.optimize_zorder",
        "jobs.vacuum",
    ]:
        imported = importlib.import_module(module)
        assert hasattr(imported, "main")

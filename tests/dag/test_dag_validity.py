"""DAG import smoke tests."""

from __future__ import annotations

import importlib


def test_dag_modules_import_without_airflow_installed() -> None:
    for module in [
        "dags.silver_pipeline",
        "dags.gold_aggregations",
        "dags.quarantine_replay",
        "dags.optimize_hot",
        "dags.optimize_zorder_nightly",
        "dags.vacuum_nightly",
    ]:
        importlib.import_module(module)

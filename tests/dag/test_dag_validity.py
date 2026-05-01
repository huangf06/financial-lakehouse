"""DAG import and structure tests."""

from __future__ import annotations

import importlib

from dags._common.callbacks import alert_on_failure

DAG_MODULES = [
    "dags.silver_pipeline",
    "dags.gold_aggregations",
    "dags.quarantine_replay",
    "dags.optimize_hot",
    "dags.optimize_zorder_nightly",
    "dags.vacuum_nightly",
]

EXPECTED_TASKS = {
    "silver_pipeline": {"bronze_to_silver_trades", "bronze_to_silver_bars"},
    "gold_aggregations": {"silver_to_gold_bars", "daily_volume_profile", "market_quality"},
    "quarantine_replay": {"replay_quarantine"},
    "optimize_hot": {"optimize_hot_partitions"},
    "optimize_zorder_nightly": {"optimize_zorder"},
    "vacuum_nightly": {"vacuum_tables"},
}


def test_dag_modules_import_without_airflow_installed() -> None:
    for module in DAG_MODULES:
        importlib.import_module(module)


def test_dag_task_sets_and_failure_callbacks() -> None:
    for module_name in DAG_MODULES:
        module = importlib.import_module(module_name)
        dags = [value for value in vars(module).values() if getattr(value, "dag_id", None)]
        assert len(dags) == 1
        dag = dags[0]
        assert {task.task_id for task in dag.tasks} == EXPECTED_TASKS[dag.dag_id]
        assert dag.default_args["on_failure_callback"] is alert_on_failure

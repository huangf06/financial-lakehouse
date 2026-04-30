"""Airflow DAG: nightly Z-order optimize."""

from __future__ import annotations

from datetime import datetime

from dags._common.operators import spark_submit_task

try:
    from airflow.decorators import dag
except Exception:  # pragma: no cover
    dag = None

if dag:

    @dag(
        dag_id="optimize_zorder_nightly",
        schedule="0 2 * * *",
        start_date=datetime(2026, 4, 30),
        catchup=False,
    )
    def _optimize_zorder_nightly():
        spark_submit_task("optimize_zorder", "/opt/app/jobs/optimize_zorder.py")

    optimize_zorder_nightly = _optimize_zorder_nightly()

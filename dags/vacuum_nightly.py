"""Airflow DAG: nightly VACUUM."""

from __future__ import annotations

from datetime import datetime

from dags._common.operators import spark_submit_task

try:
    from airflow.decorators import dag
except Exception:  # pragma: no cover
    dag = None

if dag:

    @dag(
        dag_id="vacuum_nightly",
        schedule="0 3 * * *",
        start_date=datetime(2026, 4, 30),
        catchup=False,
    )
    def _vacuum_nightly():
        spark_submit_task("vacuum_tables", "/opt/app/jobs/vacuum.py")

    vacuum_nightly = _vacuum_nightly()

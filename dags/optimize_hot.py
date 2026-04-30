"""Airflow DAG: recent-partition compaction."""

from __future__ import annotations

from datetime import datetime

from dags._common.operators import spark_submit_task

try:
    from airflow.decorators import dag
except Exception:  # pragma: no cover
    dag = None

if dag:

    @dag(
        dag_id="optimize_hot",
        schedule="0 */6 * * *",
        start_date=datetime(2026, 4, 30),
        catchup=False,
    )
    def _optimize_hot():
        spark_submit_task("optimize_hot_partitions", "/opt/app/jobs/optimize_hot.py")

    optimize_hot = _optimize_hot()

"""Airflow DAG: recent-partition compaction."""

from __future__ import annotations

from datetime import datetime, timedelta

from dags._common.callbacks import alert_on_failure
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
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": alert_on_failure,
        },
        tags=["lakehouse", "maintenance"],
    )
    def _optimize_hot():
        spark_submit_task("optimize_hot_partitions", "/opt/app/jobs/optimize_hot.py")

    optimize_hot = _optimize_hot()

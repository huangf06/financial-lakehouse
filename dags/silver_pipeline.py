"""Airflow DAG: Bronze to Silver."""

from __future__ import annotations

from datetime import datetime, timedelta

from dags._common.callbacks import alert_on_failure
from dags._common.datasets import silver_bars_ds, silver_trades_ds
from dags._common.operators import spark_submit_task

try:
    from airflow.decorators import dag
except Exception:  # pragma: no cover
    dag = None

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

if dag:

    @dag(
        dag_id="silver_pipeline",
        schedule="*/5 * * * *",
        start_date=datetime(2026, 4, 30),
        catchup=False,
        default_args=default_args,
        tags=["lakehouse", "silver"],
    )
    def _silver_pipeline():
        spark_submit_task(
            "bronze_to_silver_trades", "/opt/app/jobs/silver_trades.py", outlets=[silver_trades_ds]
        )
        spark_submit_task(
            "bronze_to_silver_bars", "/opt/app/jobs/silver_bars.py", outlets=[silver_bars_ds]
        )

    silver_pipeline = _silver_pipeline()

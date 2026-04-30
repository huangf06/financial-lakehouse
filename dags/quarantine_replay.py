"""Airflow DAG: quarantine replay."""

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
        dag_id="quarantine_replay",
        schedule="0 * * * *",
        start_date=datetime(2026, 4, 30),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": alert_on_failure,
        },
        tags=["lakehouse", "quality"],
    )
    def _quarantine_replay():
        spark_submit_task("replay_quarantine", "/opt/app/jobs/quarantine_replay.py")

    quarantine_replay = _quarantine_replay()

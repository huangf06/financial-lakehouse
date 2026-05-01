"""Airflow DAG: Silver to Gold aggregations."""

from __future__ import annotations

from datetime import datetime, timedelta

from dags._common.callbacks import alert_on_failure
from dags._common.datasets import gold_bars_5m_ds, silver_bars_ds, silver_trades_ds
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
        dag_id="gold_aggregations",
        schedule=[silver_trades_ds, silver_bars_ds],
        start_date=datetime(2026, 4, 30),
        catchup=False,
        default_args=default_args,
        tags=["lakehouse", "gold"],
    )
    def _gold_aggregations():
        spark_submit_task(
            "silver_to_gold_bars",
            "/opt/app/jobs/gold_bars.py",
            outlets=[gold_bars_5m_ds],
            env_vars={"GOLD_TIMEFRAME": "5m"},
        )
        spark_submit_task("daily_volume_profile", "/opt/app/jobs/daily_volume.py")
        spark_submit_task("market_quality", "/opt/app/jobs/market_quality.py")

    gold_aggregations = _gold_aggregations()

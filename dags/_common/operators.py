"""Shared DAG operator builders."""

from __future__ import annotations

from pathlib import Path

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except Exception:  # pragma: no cover
    SparkSubmitOperator = None  # type: ignore[assignment]


def spark_submit_task(
    task_id: str,
    application: str,
    outlets: list[object] | None = None,
    env_vars: dict[str, str] | None = None,
):
    if SparkSubmitOperator is None:
        return {
            "task_id": task_id,
            "application": application,
            "outlets": outlets or [],
            "env_vars": env_vars or {},
        }
    return SparkSubmitOperator(
        task_id=task_id,
        application=str(Path(application)),
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        env_vars=env_vars or {},
        outlets=outlets or [],
    )

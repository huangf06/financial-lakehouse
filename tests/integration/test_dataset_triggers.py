"""Airflow Dataset dependency evidence."""

from __future__ import annotations

import pytest

from dags._common.datasets import silver_bars_ds, silver_trades_ds
from dags.gold_aggregations import gold_aggregations
from dags.silver_pipeline import silver_pipeline


@pytest.mark.integration
def test_silver_dag_emits_datasets_consumed_by_gold_dag() -> None:
    silver_outlets = {
        outlet.uri for task in silver_pipeline.tasks for outlet in getattr(task, "outlets", [])
    }
    gold_inputs = {dataset.uri for dataset in gold_aggregations.timetable.dataset_condition.objects}

    assert silver_pipeline.dag_id == "silver_pipeline"
    assert gold_aggregations.dag_id == "gold_aggregations"
    assert {silver_trades_ds.uri, silver_bars_ds.uri} <= silver_outlets
    assert gold_inputs == {silver_trades_ds.uri, silver_bars_ds.uri}

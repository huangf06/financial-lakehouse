"""Spark fixtures for integration tests."""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName("integration-tests")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture
def table_path(tmp_path: Path) -> str:
    return str(tmp_path / "tables" / f"t_{uuid.uuid4().hex}")


@pytest.fixture
def checkpoint_path(tmp_path: Path) -> str:
    return str(tmp_path / "checkpoints" / f"c_{uuid.uuid4().hex}")


@pytest.fixture
def landing_path(tmp_path: Path) -> str:
    p = tmp_path / "landing"
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

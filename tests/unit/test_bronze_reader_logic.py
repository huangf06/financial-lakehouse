"""Unit tests for Databricks detection."""

from __future__ import annotations

import inspect

import pytest

from pipelines.bronze.stream_reader import is_databricks
from pipelines.bronze.writer import write_bronze_stream


def test_is_databricks_false_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert is_databricks() is False


def test_is_databricks_true_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3.x-scala2.12")
    assert is_databricks() is True


def test_write_bronze_stream_accepts_available_now_keyword() -> None:
    assert "available_now" in inspect.signature(write_bronze_stream).parameters

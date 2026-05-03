"""Unit tests for shared Airflow failure callbacks."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


class _StubResponse:
    status = 200

    def __enter__(self) -> _StubResponse:
        return self

    def __exit__(self, *args: Any) -> bool:
        return False


@pytest.fixture
def fake_context() -> dict[str, Any]:
    ti = MagicMock()
    ti.dag_id = "silver_pipeline"
    ti.task_id = "split_trades"
    return {"task_instance": ti}


def test_alert_on_failure_posts_expected_body_when_webhook_set(
    monkeypatch: pytest.MonkeyPatch, fake_context: dict[str, Any]
) -> None:
    monkeypatch.setenv("ALERTS__SLACK_WEBHOOK_URL", "https://hooks.example/T/B/X")
    captured: dict[str, Any] = {}

    def fake_urlopen(request: Any, timeout: Any) -> _StubResponse:
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _StubResponse()

    with patch("urllib.request.urlopen", fake_urlopen):
        from dags._common.callbacks import alert_on_failure

        alert_on_failure(fake_context)

    assert captured["url"] == "https://hooks.example/T/B/X"
    assert captured["headers"].get("Content-type") == "application/json"
    assert "silver_pipeline" in captured["body"]["text"]
    assert "split_trades" in captured["body"]["text"]


def test_alert_on_failure_noop_when_webhook_unset(
    monkeypatch: pytest.MonkeyPatch, fake_context: dict[str, Any]
) -> None:
    monkeypatch.delenv("ALERTS__SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    sentinel = {"called": False}

    def explode(*args: Any, **kwargs: Any) -> None:
        sentinel["called"] = True
        raise AssertionError("urlopen must not be called when webhook is unset")

    with patch("urllib.request.urlopen", explode):
        from dags._common.callbacks import alert_on_failure

        alert_on_failure(fake_context)

    assert sentinel["called"] is False


def test_alert_on_sla_miss_returns_none() -> None:
    from dags._common.callbacks import alert_on_sla_miss

    assert alert_on_sla_miss("any", positional=1) is None

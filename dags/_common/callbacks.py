"""Failure alert callbacks."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


def alert_on_failure(context: dict[str, Any]) -> None:
    webhook = os.environ.get("ALERTS__SLACK_WEBHOOK_URL") or os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    task = context.get("task_instance")
    body = {
        "text": f"Airflow task failed: {getattr(task, 'dag_id', 'unknown')}.{getattr(task, 'task_id', 'unknown')}"
    }
    request = urllib.request.Request(
        webhook,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=5)  # noqa: S310


def alert_on_sla_miss(*args: object, **kwargs: object) -> None:
    return None

"""Send a representative failure-alert payload to a real Slack webhook.

Usage:
    ALERTS__SLACK_WEBHOOK_URL=https://hooks.slack.com/... \\
        python scripts/send_test_slack_alert.py

Prints HTTP status. Intended as a one-time evidence capture.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    webhook = os.environ.get("ALERTS__SLACK_WEBHOOK_URL") or os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("ALERTS__SLACK_WEBHOOK_URL is not set", file=sys.stderr)
        return 2

    body = {
        "text": (
            "Airflow task failed: silver_pipeline.split_trades "
            "(test alert via scripts/send_test_slack_alert.py)"
        )
    }
    request = urllib.request.Request(
        webhook,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310
            print(f"HTTP {response.status}")
            return 0
    except urllib.error.HTTPError as exc:
        print(f"HTTP error: {exc.code} {exc.reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

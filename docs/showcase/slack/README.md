# Slack Failure-Alerting Evidence

Bullet 4 (`Airflow orchestration and failure alerting`) is backed by:

- The runtime callback at `dags/_common/callbacks.py::alert_on_failure`.
- Unit tests in `tests/unit/test_callbacks.py` that exercise the same function on every CI run
  and assert (a) the request URL matches the configured webhook, (b) the JSON body shape, (c)
  `dag_id` and `task_id` are rendered into the message text, (d) the callback is a no-op when
  no webhook URL is configured.
- The captured POST body (`airflow-failure-example.json`) that the live callback emits for a
  representative failure context. This is the machine-verifiable artifact that survives even
  without access to a Slack workspace.

## Capturing the Visual Screenshot (one-time human step)

The captured JSON is sufficient to defend the claim that the alert payload is well-formed. To
also produce the visual companion artifact (`airflow-failure-YYYY-MM-DD.png`):

1. Create an Incoming Webhook in any Slack workspace
   (`https://api.slack.com/apps` -> Create New App -> Incoming Webhooks -> Add New Webhook).

2. Send the test alert:

   ```bash
   ALERTS__SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
       .venv/bin/python scripts/send_test_slack_alert.py
   ```

   Expected output: `HTTP 200`.

3. Take a screenshot of the resulting Slack message and save as
   `docs/showcase/slack/airflow-failure-2026-05-XX.png`.

4. Commit the screenshot.

Once captured, the screenshot is the human-readable companion to the JSON body. Both refer to
the same code path under test in `tests/unit/test_callbacks.py`.

## Regenerating `airflow-failure-example.json`

Re-run the capture command in `scripts/send_test_slack_alert.py` source comments, or repeat
the inline Python snippet from the original commit (`docs: add Slack alert sender script and
captured POST body`). The body should always contain a `text` field naming the failed
`dag_id.task_id`.

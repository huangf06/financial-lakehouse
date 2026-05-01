"""Prometheus metrics publisher for local Delta lakehouse tables."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
from prometheus_client import Gauge, start_http_server

from metrics_publisher.queries import DELTA_TABLE_METRICS, DeltaTableMetric
from pipelines.config import Settings, load_settings


@dataclass(frozen=True)
class S3Path:
    bucket: str
    key: str


def _parse_s3a_path(path: str) -> S3Path:
    parsed = urlparse(path)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Expected s3/s3a path, got {path!r}")
    return S3Path(bucket=parsed.netloc, key=parsed.path.lstrip("/"))


def _delta_log_prefix(table_path: str) -> S3Path:
    parsed = _parse_s3a_path(table_path)
    return S3Path(parsed.bucket, f"{parsed.key.rstrip('/')}/_delta_log/")


def _record_count_from_delta_actions(lines: Iterable[str]) -> dict[str, int]:
    active_files: dict[str, int] = {}
    for line in lines:
        action = json.loads(line)
        if add := action.get("add"):
            stats = json.loads(add.get("stats") or "{}")
            active_files[add["path"]] = int(stats.get("numRecords", 0))
        if remove := action.get("remove"):
            active_files.pop(remove["path"], None)
    return active_files


class DeltaLogMetricsReader:
    def __init__(self, settings: Settings) -> None:
        storage = settings.storage
        self._settings = settings
        self._s3 = boto3.client(
            "s3",
            endpoint_url=storage.endpoint,
            aws_access_key_id=storage.access_key,
            aws_secret_access_key=storage.secret_key,
            region_name=storage.region,
        )

    def table_record_count(self, layer: str, table: str) -> int:
        table_path = self._settings.table_path(layer, table)
        delta_log = _delta_log_prefix(table_path)
        actions: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=delta_log.bucket, Prefix=delta_log.key):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".json"):
                    body = self._s3.get_object(Bucket=delta_log.bucket, Key=key)["Body"].read()
                    actions.extend(body.decode("utf-8").splitlines())
        return sum(_record_count_from_delta_actions(actions).values())


def _gauges() -> dict[str, Gauge]:
    return {
        metric.metric_name: Gauge(metric.metric_name, metric.help_text)
        for metric in DELTA_TABLE_METRICS
    }


def publish_once(
    reader: DeltaLogMetricsReader | None = None,
    metrics: list[DeltaTableMetric] | None = None,
    gauges: dict[str, Gauge] | None = None,
) -> None:
    resolved_reader = reader or DeltaLogMetricsReader(load_settings())
    resolved_metrics = metrics or DELTA_TABLE_METRICS
    resolved_gauges = gauges or _gauges()
    for metric in resolved_metrics:
        resolved_gauges[metric.metric_name].set(
            resolved_reader.table_record_count(metric.layer, metric.table)
        )


def main() -> None:
    port = int(os.environ.get("METRICS_PORT", "9108"))
    interval_sec = int(os.environ.get("METRICS_INTERVAL_SEC", "30"))
    reader = DeltaLogMetricsReader(load_settings())
    gauges = _gauges()
    start_http_server(port)
    while True:
        publish_once(reader=reader, gauges=gauges)
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()

"""Prometheus metrics publisher placeholder."""

from __future__ import annotations

import time

from prometheus_client import Gauge, start_http_server

from metrics_publisher.queries import METRIC_QUERIES

gauges = {name: Gauge(name, "Financial lakehouse metric") for name in METRIC_QUERIES}


def publish_once() -> None:
    for _name, gauge in gauges.items():
        gauge.set(0)


def main() -> None:
    start_http_server(9108)
    while True:
        publish_once()
        time.sleep(30)


if __name__ == "__main__":
    main()

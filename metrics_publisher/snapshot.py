"""Print one metrics snapshot for local validation."""

from __future__ import annotations

from metrics_publisher.publisher import DeltaLogMetricsReader
from metrics_publisher.queries import DELTA_TABLE_METRICS
from pipelines.config import load_settings


def main() -> None:
    reader = DeltaLogMetricsReader(load_settings())
    for metric in DELTA_TABLE_METRICS:
        value = reader.table_record_count(metric.layer, metric.table)
        print(f"{metric.metric_name}={value}")


if __name__ == "__main__":
    main()

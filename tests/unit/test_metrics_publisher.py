"""Metrics publisher unit tests."""

from __future__ import annotations

import json

from metrics_publisher.publisher import _parse_s3a_path, _record_count_from_delta_actions


def test_parse_s3a_path() -> None:
    parsed = _parse_s3a_path("s3a://lakehouse/silver/trades")

    assert parsed.bucket == "lakehouse"
    assert parsed.key == "silver/trades"


def test_delta_action_record_count_tracks_active_files() -> None:
    lines = [
        json.dumps({"add": {"path": "a.parquet", "stats": json.dumps({"numRecords": 10})}}),
        json.dumps({"add": {"path": "b.parquet", "stats": json.dumps({"numRecords": 5})}}),
        json.dumps({"remove": {"path": "a.parquet"}}),
        json.dumps({"add": {"path": "c.parquet", "stats": json.dumps({"numRecords": 7})}}),
    ]

    active = _record_count_from_delta_actions(lines)

    assert active == {"b.parquet": 5, "c.parquet": 7}
    assert sum(active.values()) == 12

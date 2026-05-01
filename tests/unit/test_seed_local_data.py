"""Tests for local seed helpers."""

from __future__ import annotations

import pytest

from scripts.seed_local_data import _bucket_and_prefix


def test_bucket_and_prefix_parses_lakehouse_path() -> None:
    assert _bucket_and_prefix("s3a://lakehouse/landing/binance") == (
        "lakehouse",
        "landing/binance",
    )


def test_bucket_and_prefix_rejects_non_s3_path() -> None:
    with pytest.raises(ValueError, match="Expected s3/s3a path"):
        _bucket_and_prefix("/tmp/landing/binance")

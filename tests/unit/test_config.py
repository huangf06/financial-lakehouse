"""Tests for config profile loading."""

from __future__ import annotations

import pytest

from pipelines.config import load_settings


def test_load_compose_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE", "compose")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("S3_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("LAKEHOUSE_ROOT", "s3a://lakehouse")
    monkeypatch.setenv("CHECKPOINT_ROOT", "s3a://lakehouse-meta/_checkpoints")
    settings = load_settings()
    assert settings.profile == "compose"
    assert settings.storage.endpoint == "http://minio:9000"
    assert settings.landing_path("binance") == "s3a://lakehouse/landing/binance"


def test_invalid_profile_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE", "made-up")
    with pytest.raises(ValueError, match="profile"):
        load_settings()

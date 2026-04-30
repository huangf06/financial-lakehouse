"""Loader that maps PROFILE env var to a Settings instance."""

from __future__ import annotations

import os
from typing import get_args

from pipelines.config.settings import ProfileName, Settings, StorageConfig


def load_settings() -> Settings:
    profile = os.environ.get("PROFILE", "compose")
    if profile not in get_args(ProfileName):
        raise ValueError(f"Unknown profile {profile!r}; valid: {get_args(ProfileName)}")

    storage = StorageConfig(
        endpoint=os.environ.get("S3_ENDPOINT"),
        region=os.environ.get("S3_REGION", "us-east-1"),
        access_key=os.environ.get("S3_ACCESS_KEY"),
        secret_key=os.environ.get("S3_SECRET_KEY"),
        lakehouse_root=os.environ.get("LAKEHOUSE_ROOT", "s3a://lakehouse"),
        checkpoint_root=os.environ.get("CHECKPOINT_ROOT", "s3a://lakehouse-meta/_checkpoints"),
        path_style_access=os.environ.get("S3_PATH_STYLE", "true").lower() == "true",
    )
    return Settings(profile=profile, storage=storage)  # type: ignore[arg-type]

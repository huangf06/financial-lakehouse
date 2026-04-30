"""Profile-based configuration with strict validation."""
# ruff: noqa: UP007

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ProfileName = Literal["compose", "ci", "aws-showcase", "personal-frugal", "databricks"]


class StorageConfig(BaseModel):
    endpoint: Optional[str] = None
    region: str = "us-east-1"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    lakehouse_root: str = Field(..., description="e.g. s3a://lakehouse")
    checkpoint_root: str = Field(..., description="e.g. s3a://lakehouse-meta/_checkpoints")
    path_style_access: bool = True


class SparkConfig(BaseModel):
    master: str = "local[2]"
    executor_memory: str = "2g"
    driver_memory: str = "2g"


class AirflowConfig(BaseModel):
    db_url: str = "postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow"


class AlertConfig(BaseModel):
    slack_webhook_url: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_file=None,
        case_sensitive=False,
        extra="ignore",
    )

    profile: ProfileName = "compose"
    storage: StorageConfig
    spark: SparkConfig = SparkConfig()
    airflow: AirflowConfig = AirflowConfig()
    alerts: AlertConfig = AlertConfig()

    def landing_path(self, source: str) -> str:
        return f"{self.storage.lakehouse_root}/landing/{source}"

    def schema_path(self, source: str) -> str:
        return f"{self.storage.lakehouse_root}/_schemas/{source}"

    def checkpoint(self, query: str) -> str:
        return f"{self.storage.checkpoint_root}/{query}"

    def table_path(self, layer: str, name: str) -> str:
        return f"{self.storage.lakehouse_root}/{layer}/{name}"

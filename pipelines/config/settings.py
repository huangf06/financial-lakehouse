"""Profile-based configuration with strict validation."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ProfileName = Literal["compose", "ci", "aws-showcase", "personal-frugal", "databricks"]


class StorageConfig(BaseModel):
    endpoint: str | None = None
    region: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None
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
    slack_webhook_url: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None


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

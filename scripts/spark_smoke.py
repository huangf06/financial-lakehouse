"""Spark smoke test reading landing/binance from MinIO."""

from __future__ import annotations

from jobs._common import spark_session
from pipelines.config import load_settings


def main() -> None:
    settings = load_settings()
    spark = spark_session("smoke-test", settings)
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("recursiveFileLookup", "true").json(settings.landing_path("binance"))
    count = df.count()
    print(f"=== SMOKE TEST: read {count} records ===")
    assert count > 0
    df.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

"""Count rows in the Bronze Delta table for the replay/binance source."""

from __future__ import annotations

from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.captured import AnalysisException

from jobs._common import spark_session
from pipelines.config import load_settings


def main() -> None:
    settings = load_settings()
    spark = spark_session("bronze-replay-count", settings)
    spark.sparkContext.setLogLevel("WARN")
    path = settings.table_path("bronze", "binance_replay_trades")
    try:
        df = spark.read.format("delta").load(path)
        print(f"=== BRONZE REPLAY COUNT: {df.count()} records ===")
    except (Py4JJavaError, AnalysisException) as exc:
        message = str(exc)
        if (
            "DELTA_TABLE_NOT_FOUND" in message
            or "PATH_NOT_FOUND" in message
            or "Path does not exist" in message
        ):
            print("=== BRONZE REPLAY COUNT: table not found ===")
        else:
            raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

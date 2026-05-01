"""Small reproducible benchmark harness for compaction and Z-order claims."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

QUERIES = [
    "q1_single_symbol_24h.sql",
    "q2_single_symbol_7d_vwap.sql",
    "q3_cross_section_1h.sql",
    "q4_count_by_symbol.sql",
]


def _spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("optimization-benchmark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _seed_table(spark: SparkSession, path: str, rows: int) -> None:
    symbol = (
        "CASE pmod(id, 8) "
        "WHEN 0 THEN 'BTCUSDT' WHEN 1 THEN 'ETHUSDT' WHEN 2 THEN 'SOLUSDT' "
        "WHEN 3 THEN 'AAPL' WHEN 4 THEN 'MSFT' WHEN 5 THEN 'NVDA' "
        "WHEN 6 THEN 'SPY' ELSE 'QQQ' END"
    )
    df = (
        spark.range(rows)
        .selectExpr(
            "timestampadd(MINUTE, -cast(pmod(id, 10080) as int), current_timestamp()) AS event_ts",
            "current_timestamp() AS ingest_ts",
            "date_sub(current_date(), cast(pmod(id, 7) as int)) AS event_date",
            "'binance' AS source",
            "'crypto' AS asset_class",
            f"{symbol} AS symbol",
            "CASE WHEN pmod(id, 2) = 0 THEN 'buy' ELSE 'sell' END AS side",
            "cast(50000 + pmod(id, 20000) * 0.01 AS decimal(38,18)) AS price",
            "cast(0.001 + pmod(id, 500) * 0.0001 AS decimal(38,18)) AS quantity",
            "concat('binance:', cast(id as string)) AS trade_id",
            "cast(pmod(id, 120) as int) AS late_arrival_sec",
        )
        .withColumn("notional", col("price") * col("quantity"))
        .withColumn("_raw_json", concat(lit('{"id":'), col("trade_id"), lit("}")))
    )
    (
        df.repartition(48, "symbol")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(path)
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("DROP TABLE IF EXISTS silver.trades")
    spark.sql(f"CREATE TABLE silver.trades USING DELTA LOCATION '{path}'")


def _query_texts() -> dict[str, str]:
    query_dir = Path(__file__).parent / "queries"
    return {name: (query_dir / name).read_text(encoding="utf-8") for name in QUERIES}


def _time_queries(
    spark: SparkSession, queries: dict[str, str], iterations: int
) -> list[dict[str, float | str]]:
    timings = []
    for name, sql in queries.items():
        runs = []
        for _ in range(iterations):
            started = time.perf_counter()
            spark.sql(sql).collect()
            runs.append((time.perf_counter() - started) * 1000)
        timings.append(
            {
                "name": name,
                "median_ms": round(statistics.median(runs), 2),
                "min_ms": round(min(runs), 2),
                "max_ms": round(max(runs), 2),
            }
        )
    return timings


def _benchmark(spark: SparkSession, rows: int, iterations: int) -> dict[str, object]:
    with TemporaryDirectory(prefix="lakehouse-benchmark-") as tmp:
        table_path = str(Path(tmp) / "silver_trades")
        _seed_table(spark, table_path, rows)
        queries = _query_texts()
        result: dict[str, object] = {
            "timestamp": int(time.time()),
            "rows": rows,
            "iterations": iterations,
            "table_path": table_path,
            "stages": [],
        }
        stages = result["stages"]
        assert isinstance(stages, list)

        stages.append({"name": "baseline", "queries": _time_queries(spark, queries, iterations)})
        spark.sql("OPTIMIZE silver.trades").collect()
        stages.append({"name": "compact", "queries": _time_queries(spark, queries, iterations)})
        spark.sql("OPTIMIZE silver.trades ZORDER BY (symbol)").collect()
        stages.append(
            {"name": "zorder_symbol", "queries": _time_queries(spark, queries, iterations)}
        )
        return result


def _write_markdown(result: dict[str, object], path: Path) -> None:
    stages = result["stages"]
    assert isinstance(stages, list)
    rows = ["# Optimization Benchmark Results", ""]
    rows.append(f"- Rows: {result['rows']}")
    rows.append(f"- Iterations per query: {result['iterations']}")
    rows.append(f"- Timestamp: {result['timestamp']}")
    rows.append("")
    rows.append("| Query | Baseline median ms | Compact median ms | Z-order median ms |")
    rows.append("|---|---:|---:|---:|")
    by_stage = {
        stage["name"]: {query["name"]: query for query in stage["queries"]}  # type: ignore[index]
        for stage in stages
    }
    for query in QUERIES:
        rows.append(
            "| "
            + " | ".join(
                [
                    query,
                    str(by_stage["baseline"][query]["median_ms"]),
                    str(by_stage["compact"][query]["median_ms"]),
                    str(by_stage["zorder_symbol"][query]["median_ms"]),
                ]
            )
            + " |"
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()

    spark = _spark()
    try:
        result = _benchmark(spark, rows=args.rows, iterations=args.iterations)
    finally:
        spark.stop()

    out = Path("benchmarks/raw")
    out.mkdir(parents=True, exist_ok=True)
    raw_path = out / f"{result['timestamp']}.json"
    raw_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    _write_markdown(result, Path("benchmarks/results.md"))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

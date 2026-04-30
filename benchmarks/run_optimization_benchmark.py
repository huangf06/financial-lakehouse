"""Small reproducible benchmark harness for compaction and Z-order claims."""

from __future__ import annotations

import json
import time
from pathlib import Path

QUERIES = [
    "q1_single_symbol_24h.sql",
    "q2_single_symbol_7d_vwap.sql",
    "q3_cross_section_1h.sql",
    "q4_count_by_symbol.sql",
]


def main() -> None:
    result = {
        "timestamp": int(time.time()),
        "note": "Run against a prepared Delta table with Spark to collect real timings.",
        "queries": [{"name": query, "median_ms": None} for query in QUERIES],
    }
    out = Path("benchmarks/raw")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{result['timestamp']}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

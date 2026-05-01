# Optimization Benchmark

Run `uv run python benchmarks/run_optimization_benchmark.py` to generate a local synthetic
Silver trades Delta table and measure the query set across baseline, compact-only, and
Z-order states. The harness writes raw JSON under `benchmarks/raw/` and updates
`benchmarks/results.md`.

# Silver Replay Progress

Date: 2026-05-01

## Completed

- Added `make replay-demo`, backed by `scripts/replay_demo.py`.
- The demo writes a Bronze-shaped Delta table, runs the real Silver trade split, quarantines one row on `SYMBOL_UNKNOWN`, then replays it with a relaxed rule set and writes it into a demo Silver Delta table.
- Replaced the previous end-to-end placeholder integration test with a real local Delta loop covering Bronze-shaped input, Silver quarantine, replay, and final Silver output.
- Extended `jobs._common.write_delta()` with an explicit write mode parameter so demo/test flows can create isolated overwriteable Delta tables.
- Kept Docker runtime compatibility with the current Spark container Python 3.8 by avoiding Python 3.11-only runtime constructs in code executed inside the container.

## Validation

```text
.venv/bin/python -m pytest tests/unit tests/dag tests/integration -q
29 passed in 68.47s

.venv/bin/ruff check .
All checks passed!

.venv/bin/ruff format --check .
108 files already formatted

.venv/bin/mypy
Success: no issues found in 62 source files

docker compose config --quiet
passed

make replay-demo
=== REPLAY DEMO ===
before: silver=0, quarantine=1
after: silver=1, quarantine=0
replayed_trade_ids=['binance:9001']
```

# Optimization Benchmark Results

- Rows: 100000
- Iterations per query: 3
- Timestamp: 1777635891

| Query | Baseline median ms | Compact median ms | Z-order median ms |
|---|---:|---:|---:|
| q1_single_symbol_24h.sql | 791.57 | 508.73 | 477.5 |
| q2_single_symbol_7d_vwap.sql | 753.28 | 490.54 | 518.47 |
| q3_cross_section_1h.sql | 835.0 | 547.57 | 509.75 |
| q4_count_by_symbol.sql | 577.84 | 454.77 | 403.33 |

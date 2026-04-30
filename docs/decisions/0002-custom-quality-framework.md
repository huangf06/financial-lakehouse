# ADR 0002: Custom Quality Framework

## Status

Accepted.

## Decision

Use a small declarative rule framework instead of Great Expectations or Pandera.

## Consequences

Rules are visible as code and easy to test with Spark DataFrames. The tradeoff is fewer built-in reports than larger validation frameworks.

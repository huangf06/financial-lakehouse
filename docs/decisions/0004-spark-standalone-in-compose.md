# ADR 0004: Spark Standalone In Compose

## Status

Accepted.

## Decision

Use Spark Standalone locally rather than embedding all jobs in a single Python container.

## Consequences

Local development looks closer to a real distributed Spark deployment while remaining reproducible with Docker Compose.

# ADR 0001: Medallion Lakehouse Instead Of Pure Kappa

## Status

Accepted.

## Decision

Use Bronze, Silver, and Gold Delta layers. Bronze preserves raw data, Silver enforces quality and normalized schemas, and Gold serves analytical tables.

## Consequences

The architecture is easier to inspect and explain in interviews than a single streaming-only topology, at the cost of more tables and orchestration.

# ADR 0007: Custom Metrics Publisher

## Status

Accepted.

## Decision

Publish a small Prometheus bridge for lakehouse health metrics instead of adding SQL plugins to Grafana.

## Consequences

Grafana provisioning stays stable and lightweight. The publisher owns Delta query scheduling and metric shape.

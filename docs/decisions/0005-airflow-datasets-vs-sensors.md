# ADR 0005: Airflow Datasets Instead Of Sensors

## Status

Accepted.

## Decision

Use Airflow Datasets for producer-consumer DAG dependencies.

## Consequences

Dependencies are declarative and visible in DAG definitions. This avoids polling-style `ExternalTaskSensor` wiring.

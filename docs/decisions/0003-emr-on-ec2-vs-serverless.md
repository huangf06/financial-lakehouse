# ADR 0003: EMR On EC2 For Showcase

## Status

Accepted for future AWS showcase work.

## Decision

Prefer EMR on EC2 for a short-lived production-style demo because it exposes Spark, IAM, VPC, and operational surfaces directly.

## Consequences

The profile is more resume-defensible but requires careful teardown to control costs.

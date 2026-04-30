# ADR 0006: Bronze Is Append-Only And Not Deduplicated

## Status

Accepted.

## Decision

Bronze preserves raw source records and metadata. Deduplication and quality decisions happen downstream.

## Consequences

Reprocessing remains auditable, but consumers must use Silver or Gold for cleaned data.

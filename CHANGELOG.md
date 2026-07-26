# Changelog

## v0.1.0 — 2026-07-25

Initial public release of the controlled local reference implementation.

- Transactional-outbox CDC, Kafka lineage ingestion, Spark affected-record discovery
- Hashed dry-run plans, parallel idempotent workers, checkpoints, crash recovery
- Optimistic concurrent-write protection, validation, append-only audit, compensation
- Payment, ledger-entry, and fee lifecycle with real Kafka/PostgreSQL experiments
- Versioned 100,000-record, hard-fault, and local Kind evidence

This release is not a production deployment or availability claim.

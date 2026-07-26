# Architecture and failure semantics

## Selection and lineage

The payment transaction and its outbox insert share one PostgreSQL commit. The publisher deliberately offers at-least-once semantics: a process failure after Kafka acknowledgement but before setting `published_at` results in a replay. `record_lineage.event_id` is the idempotency boundary, so the replay is observable in Kafka but cannot duplicate lineage.

Each lineage row identifies the record, source database transaction, event kind, responsible code and configuration versions, Kafka position, and event payload. Discovery joins this immutable history to the current payment row. The plan captures the current row version and exact before/after images; its hash changes if any selected content changes.

## Mutation protocol

1. A worker atomically claims one pending item with `FOR UPDATE SKIP LOCKED`.
2. It updates the payment only when `row_version = expected_row_version`.
3. A version mismatch becomes a terminal, audited conflict. The engine never retries against an unreviewed new state.
4. The worker appends balanced merchant/platform ledger deltas.
5. It verifies the target values, payment equation, and zero-sum ledger.
6. On failure, it compare-and-swaps the before-image back and appends inverse ledger entries.
7. Item state, checkpoint, audit action, target mutation, and ledger changes share one transaction.

The deterministic item key and `(job_id, payment_id)` primary key prevent the same planned repair from being created twice. Terminal item states are never selected by a rerun.

## Crash matrix

| Failure point | Durable state | Recovery |
|---|---|---|
| Before item claim commits | `PENDING` | Any worker claims it |
| After claim, before repair transaction | `APPLYING` | Stale-claim recovery resets it to `PENDING` |
| During repair transaction | `APPLYING`, no partial target write | Database rollback, then stale-claim recovery |
| After repair commit | `APPLIED` plus checkpoint/audit | Rerun skips it |
| Kafka send before outbox acknowledgement | Outbox may remain unpublished | Relay repeats; lineage event-id dedupes |
| Legitimate concurrent write after planning | New payment row version | Repair CAS fails and audits `CONCURRENT_WRITE_CONFLICT` |

## Trust boundaries

PostgreSQL constraints are the final guard for monetary equations. Kafka is a durable transport, not the source of truth. Spark output is a scalable review/discovery artifact, not authorization to mutate. A persisted, non-dry-run plan is the mutation authorization boundary.

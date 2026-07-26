# Controlled Local Experiment Results

## Recruiting-scale experiment

The final controlled scale run was generated at 2026-07-26T01:09:38.388376Z on a macOS arm64 host with a Java 26.0.1 runtime and Java 17-targeted application. Kafka 4.0 and PostgreSQL 17 ran in real Docker containers.

| Metric | Measured result |
|---|---:|
| Corrupted payments | 100,000 |
| Repair workers | 8 |
| Repair-worker time | 115.613 s |
| Repair throughput | **864.95 records/s** |
| SQL planner discovery | 266,128.52 records/s |
| Spark discovery | 71,343.66 records/s |
| Spark Parquet write | 1.151 s, 8 partitions |
| Ordinary writes during plan/repair | 8,787 successful, 0 errors |
| Baseline write p50 / p95 | 0.857 ms / 1.504 ms |
| Concurrent write p50 / p95 | 2.673 ms / 4.979 ms |
| p50 / p95 impact | +211.85% / +231.01% |
| CAS conflicts | 92 |
| Follow-up reviewed records | 93 |
| Repair retries | 1 |
| Automatic rollbacks | 1 |
| Duplicate events redelivered | 2,000 |
| Duplicate lineage / repair applies | 0 / 0 |
| Wrong fees / invalid payments / unbalanced ledgers | 0 / 0 / 0 |

Throughput covers repair-engine worker time, including conflict recovery and the reviewed follow-up plan. It excludes seeding, CDC, and discovery. The latency comparison shows material impact, so this project does not claim zero impact or production zero downtime.

Sources:

- `evidence/scale-100k.json`
- `evidence/spark-discovery-100k.json`

## Hard fault-injection experiment

The final 5,000-record process/service fault run:

- SIGKILLed one worker after checkpoints were durable.
- Restarted Kafka twice during CDC.
- Restarted PostgreSQL during repair.
- Redelivered 1,500 duplicate Kafka events across the two replay steps.
- Automatically retried database failures and recovered the abandoned item.
- Completed 5,000/5,000 checkpoints with four retries, zero duplicate applies, zero wrong fees, and valid final invariants.

Source: `evidence/fault-injection.json`. Service interruption latency was not measured, so this result is recovery evidence, not an availability claim.

## Kind Kubernetes experiment

A disposable Kind v0.27.0 cluster ran local single-platform images for PostgreSQL, Kafka, Spark, and the repair engine:

- Spark discovered 5,000 affected records inside the cluster.
- Four worker pods became ready and five distinct worker identities processed work after replacement.
- One ready worker pod was force-deleted after checkpoints existed.
- Durable crash recovery was audited 12 seconds after deletion.
- All 5,000 records completed once with one retry and zero wrong fees.
- Worker pods were verified as non-root, read-only-root-filesystem, no privilege escalation, dropped capabilities, no service-account token, and an executable readiness probe.
- The Kind cluster was deleted after the run.

Source: `evidence/kind-e2e.json`.

## Preserved baseline

The original controlled run remains at `evidence/baseline.json`: 1,000 records at 380.08 records/s, one logical abandoned claim, one conflict, one rollback, and valid final invariants. It was not overwritten by the scale work.

## Automated tests

- Unit: 4 passed, 0 failed.
- Real Kafka/PostgreSQL integration: 3 passed, 0 failed.
- Hard fault harness: passed.
- Kind end-to-end and forced pod recovery: passed.
- Kustomize/client security assertions: passed.

Coverage includes monetary edge cases, authorization/capture/refund, lineage attribution, duplicate CDC, dry-run rejection, idempotency, append-only audit enforcement, checkpoints, rollback, concurrent-write protection, hard worker death, service restarts, and pod replacement.

## Boundaries and limitations

- These are controlled local experiments, not production deployments or external adoption.
- The 100,000-record scale run and 5,000-record hard-fault/Kind runs are separate experiments.
- Kafka and PostgreSQL each use one local instance; broker replication, PostgreSQL HA/failover, and multi-node partitions were not tested.
- CDC is a transactional-outbox relay rather than PostgreSQL logical decoding/Debezium.
- Automatic rollback is per record; job-wide reversal of previously committed repairs is not implemented.
- Full settlement, dispute, chargeback, PCI controls, retention/redaction, and operator approval workflows remain out of scope.
- The unrelated pre-existing Kind cluster `reproci-validation` was not removed; the task-created `repair-engine` cluster, all project Compose containers, and project volumes were removed.

## Exact next steps

1. Test replicated Kafka and PostgreSQL HA/failover, including leader promotion and network partitions.
2. Run 1M+ records on fixed, documented hardware and separate end-to-end throughput from worker-only throughput.
3. Add logical-decoding/Debezium ingestion and reconcile it against the outbox.
4. Add job-wide rollback orchestration and operator approval/pause/cancel controls.
5. Add settlement, dispute, chargeback, encryption, retention/redaction, metrics, and distributed tracing.

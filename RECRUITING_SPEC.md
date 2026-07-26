# Recruiting Specification and Evidence Ledger

This file is the source of truth for the intended resume entry. A claim is supported only when the repository contains an implementation and a reproducible local test or experiment that demonstrates it. Measurements must be copied from generated experiment artifacts; placeholders must never be presented as results.

## Intended resume entry

> **Production Data Repair Engine | Java, Kafka, PostgreSQL, Spark, Kubernetes**  
> Built a production-safe engine that traces corrupted records to code or configuration changes, previews affected data, and performs online repairs without stopping services.  
> Implemented CDC, lineage, idempotent backfills, checkpoint recovery, validation, audit logging, and automatic rollback.  
> Repaired **[N]** records at **[X] records/second** while preserving consistency across crashes, duplicate events, and concurrent writes.

## Evidence standard

- **Supported**: implementation plus a passing, reproducible automated test or recorded experiment.
- **Partially supported**: implementation exists, but the required real-service or failure-mode evidence is incomplete.
- **Unsupported**: no implementation or no valid local evidence.
- External adoption, production deployment, and real customer impact are out of scope and must not be implied.

## Claim ledger

| ID | Claim | Required evidence | Status | Evidence |
|---|---|---|---|---|
| C1 | Uses Java, Kafka, PostgreSQL, Spark, and Kubernetes | Build manifests, runtime composition, Spark discovery job, Kubernetes manifests/tests | **Supported** | Java 17 target, Kafka 4.0, PostgreSQL 17, Spark 4.0, Docker images, and Kind manifests/scripts all executed locally. Spark independently found 100,000 rows in eight partitions |
| C2 | Models a realistic payments workload | Ledger-preserving payment state machine, authorization/capture/refund events, monetary invariants | **Supported** | Atomic authorization, capture, partial/full refund, fee, outbox, and three-account ledger transitions are implemented. Lifecycle integration test passed with rejected duplicate capture and over-refund |
| C3 | Traces corrupted records to responsible code/configuration versions | Immutable CDC envelope contains code/config versions; lineage queries identify implicated versions | **Supported** | Lineage retains event/record IDs, source transaction, code/config versions, Kafka partition/offset, and payload. Planner and Spark selected `bad-fees-v2`; metadata integration assertions passed |
| C4 | Previews affected data before mutation | Dry-run plan with counts, samples, expected before/after values, and stable plan hash | **Supported** | 100,000-row preview generated SHA-256 plan `6a1fe...402e4`, exact before/after JSON, and zero job-scoped repair mutation events while ordinary writes continued |
| C5 | Performs online repairs without stopping services | Repair workers operate while workload writes continue; optimistic version protection prevents clobbering | **Supported with measured impact** | 8,787 ordinary writes completed with zero errors during discovery/repair. p50 changed 0.857→2.673 ms and p95 1.504→4.979 ms; 92 stale plans conflicted safely. This is not described as zero-impact or production zero-downtime |
| C6 | CDC ingestion | Transactional outbox publisher to Kafka plus idempotent lineage consumer | **Supported** | Scale run ingested 110,000 initial plus 8,988 concurrent-write events; hard-fault run restarted Kafka twice without lost lineage |
| C7 | Idempotent parallel backfills | Deterministic repair key, database uniqueness, worker partition claims, repeated-run test | **Supported** | Eight-worker 100,000-row run, repeat-run integration assertion, 100,000 successful unique applies, and zero duplicate repair applies |
| C8 | Checkpoint and crash recovery | Durable job/partition checkpoints and forced worker crash/restart experiment | **Supported** | Local harness SIGKILLed a worker after checkpoints and restarted PostgreSQL; automatic survivor recovery finished 5,000/5,000 with four retries. Kind force-deleted a worker pod and recovered its stale claim |
| C9 | Validation | Per-record postconditions and payment/ledger aggregate invariants | **Supported** | Scale, hard-fault, and Kind reports all ended with zero invalid payments, unbalanced ledgers, duplicate lineage, or wrong fees |
| C10 | Audit logging | Append-only audit events for plans, attempts, conflicts, validation, commit, and rollback | **Supported** | Database trigger rejects audit update/delete; integration test verifies enforcement; recovery, conflict, apply, and rollback actions are recorded |
| C11 | Concurrent-write protection | Compare-and-swap update against discovered row version; conflict is audited and not overwritten | **Supported** | Sustained-writer scale run measured 92 conflicts, then reviewed current state in a follow-up plan of 93 rows; final wrong-fee count was zero |
| C12 | Automatic rollback | Before-images retained and compensating compare-and-swap rollback on validation failure | **Supported** | Scale run injected and automatically rolled back one invalid repair; baseline explicitly verified restored business state and balanced compensating ledger entries |
| C13 | Preserves consistency across duplicate events | Duplicate Kafka delivery produces one lineage fact and no duplicate repair | **Supported** | Scale run redelivered 2,000 events and ended with zero duplicate lineage and zero duplicate repair applies; Kafka integration test also passed |
| C14 | Repairs **[N]** records at **[X] records/second** | Machine-generated experiment JSON with inputs, elapsed time, completed count, environment, and invariant results | **Supported with local qualifier** | `repair-scale-experiment.json`: **100,000 records at 864.95 records/second** over 115.613 seconds of repair-worker time on the local macOS arm64 host |
| C15 | Recovery time | Forced crash timestamp and first successful post-restart progress/completion timestamp | **Supported** | Kind recorded 12,000 ms from force-deleting a ready worker pod to durable `CRASH_RECOVERY`; all 5,000 items completed once |
| C16 | Rollback behavior | Injected invalid repair causes automatic rollback; before/after hashes and invariants match | **Supported** | One injected rollback, zero duplicate applies, zero wrong fees, and final ledger/payment invariants passed |
| C17 | Kubernetes readiness | Valid manifests plus automated render/schema or ephemeral-cluster smoke test | **Supported** | Disposable Kind cluster ran PostgreSQL, Kafka, Spark, and four worker pods; verified four ready pods, five worker identities after replacement, pod recovery, non-root/read-only/no-escalation/no-token settings, and successful validation |

## Evidence-backed local wording

The placeholder throughput sentence may be replaced only with this qualified statement:

> In a controlled local Kafka/PostgreSQL experiment, repaired **100,000** corrupted payment records at **864.95 records/second** of repair-worker time while 8,787 ordinary writes continued; separate fault runs preserved invariants across worker SIGKILL, Kafka/PostgreSQL restarts, 2,000 duplicate deliveries, concurrent-write conflicts, and Kubernetes pod replacement.

This wording must retain “controlled local” and “separate fault runs.” It must not be presented as production deployment, external adoption, zero downtime, or a general capacity result.

## Final experiment ledger

| Experiment | Artifact | Result |
|---|---|---|
| Preserved baseline | `evidence/baseline.json` | 1,000 records, 380.08 records/s; original evidence retained unchanged |
| Recruiting-scale run | `evidence/scale-100k.json` | 100,000 records, 864.95 records/s; 8,787 concurrent writes; 92 conflicts; all invariants valid |
| Spark scale discovery | `evidence/spark-discovery-100k.json` | 100,000 rows, 71,343.66 records/s, eight partitions, 1.151 s Parquet write |
| Hard service/process faults | `evidence/fault-injection.json` | 1 SIGKILL, 2 Kafka restarts, 1 PostgreSQL restart, 4 retries, 5,000/5,000 checkpoints, zero duplicates |
| Kind end-to-end | `evidence/kind-e2e.json` | 4 ready pods, 5 worker identities, 1 force deletion, 12 s recovery, 5,000/5,000 applies |
| Automated tests | Maven and Kubernetes scripts | 4 unit + 3 real-service integration tests; manifest security test passed |

## Measurement protocol

The canonical experiment must:

1. Start real Kafka and PostgreSQL containers from a clean state.
2. Generate authorization, capture, and refund traffic with a deliberately bad fee configuration version.
3. Publish transactional-outbox CDC and intentionally replay duplicate Kafka records.
4. Discover the affected set through the Spark job and persist a hashed repair plan.
5. Run a dry preview and prove it performs no target-row writes.
6. Run multiple repair workers while concurrent legitimate payment writes continue.
7. Kill a worker after a durable checkpoint, restart it, and measure recovery.
8. Inject at least one validation failure, demonstrate automatic rollback, then run a valid repair.
9. Verify row-level expected values, ledger balance invariants, idempotency, audit completeness, and conflict handling.
10. Write scale measurements to `build/reports/repair-scale-experiment.json`, then review and curate the final result under `evidence/`; preserve the original baseline separately.

## Reporting rule

The final project report must list measured evidence, unsupported or partially supported claims, limitations, and exact next steps. It must not claim external adoption, production deployment, or measurements that were not produced locally.

package dev.datarepair.cli;

import dev.datarepair.experiment.EndToEndExperiment;
import dev.datarepair.experiment.ScaleExperiment;
import dev.datarepair.cdc.LineageConsumer;
import dev.datarepair.cdc.OutboxPublisher;
import dev.datarepair.db.Database;
import dev.datarepair.repair.InvariantValidator;
import dev.datarepair.repair.RepairEngine;
import dev.datarepair.repair.RepairPlanner;
import dev.datarepair.workload.PaymentWorkload;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;

@Command(name = "repair-engine", mixinStandardHelpOptions = true,
        description = "Production-safe live data repair engine",
        subcommands = {RepairCli.Experiment.class, RepairCli.Scale.class,
                RepairCli.Initialize.class, RepairCli.Seed.class,
                RepairCli.Publish.class, RepairCli.Consume.class,
                RepairCli.Plan.class, RepairCli.Worker.class,
                RepairCli.Validate.class})
public final class RepairCli implements Runnable {
    public static void main(String[] args) {
        System.exit(new CommandLine(new RepairCli()).execute(args));
    }

    abstract static class DatabaseCommand {
        @Option(names = "--jdbc") String jdbc =
                environment("JDBC_URL", "jdbc:postgresql://localhost:54329/repairs");
        @Option(names = "--user") String user = environment("DB_USER", "repairs");
        @Option(names = "--password") String password = environment("DB_PASSWORD", "repairs");

        javax.sql.DataSource dataSource() {
            return Database.dataSource(jdbc, user, password);
        }
    }

    @Command(name = "initialize", description = "Create or update the database schema")
    static final class Initialize extends DatabaseCommand implements Callable<Integer> {
        @Override public Integer call() throws Exception {
            Database.initialize(dataSource());
            System.out.println("SCHEMA_INITIALIZED=true");
            return 0;
        }
    }

    @Command(name = "seed", description = "Seed ledger-backed captured payments")
    static final class Seed extends DatabaseCommand implements Callable<Integer> {
        @Option(names = "--corrupted", defaultValue = "5000") int corrupted;
        @Option(names = "--controls", defaultValue = "500") int controls;
        @Option(names = "--chunk", defaultValue = "1000") int chunk;

        @Override public Integer call() throws Exception {
            var ds = dataSource();
            Database.initialize(ds);
            var workload = new PaymentWorkload(ds);
            workload.generateCapturesBatched(controls, chunk, 101,
                    "payments-3.0.0", "fees-good-v3", 290);
            workload.generateCapturesBatched(corrupted, chunk, 103,
                    "payments-2.9.0", "bad-fees-v2", 790);
            System.out.printf("SEEDED_CORRUPTED=%d%nSEEDED_CONTROLS=%d%n",
                    corrupted, controls);
            return 0;
        }
    }

    @Command(name = "publish", description = "Drain transactional outbox to Kafka")
    static final class Publish extends DatabaseCommand implements Callable<Integer> {
        @Option(names = "--kafka") String kafka =
                environment("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        @Option(names = "--replay", defaultValue = "0") int replay;
        @Option(names = "--retry-seconds", defaultValue = "300") long retrySeconds;
        @Option(names = "--batch-size", defaultValue = "5000") int batchSize;
        @Option(names = "--batch-delay-ms", defaultValue = "0") long batchDelayMs;

        @Override public Integer call() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(retrySeconds).toNanos();
            int failures = 0;
            while (true) {
                try (var publisher = new OutboxPublisher(dataSource(), kafka)) {
                    int published = publisher.drain(batchSize,
                            Duration.ofMillis(batchDelayMs));
                    if (replay > 0) publisher.replayPublished(replay);
                    System.out.printf("PUBLISHED=%d%nREPLAYED=%d%nPUBLISH_RETRIES=%d%n",
                            published, replay, failures);
                    return 0;
                } catch (Exception transientFailure) {
                    failures++;
                    if (System.nanoTime() >= deadline) throw transientFailure;
                    System.err.printf("PUBLISH_RETRY=%d cause=%s%n", failures,
                            transientFailure.getClass().getSimpleName());
                    Thread.sleep(Math.min(5_000, failures * 250L));
                }
            }
        }
    }

    @Command(name = "consume", description = "Consume Kafka CDC into immutable lineage")
    static final class Consume extends DatabaseCommand implements Callable<Integer> {
        @Option(names = "--kafka") String kafka =
                environment("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        @Option(names = "--group", defaultValue = "repair-lineage") String group;
        @Option(names = "--maximum-seconds", defaultValue = "300") long maximumSeconds;

        @Override public Integer call() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(maximumSeconds).toNanos();
            int inserted = 0;
            int failures = 0;
            while (System.nanoTime() < deadline) {
                try (var consumer = new LineageConsumer(dataSource(), kafka, group)) {
                    inserted += consumer.consumeUntilIdle(
                            Duration.ofMillis(500), Duration.ofSeconds(maximumSeconds));
                    System.out.printf("LINEAGE_INSERTED=%d%nCONSUME_RETRIES=%d%n",
                            inserted, failures);
                    return 0;
                } catch (Exception transientFailure) {
                    failures++;
                    System.err.printf("CONSUME_RETRY=%d cause=%s%n", failures,
                            transientFailure.getClass().getSimpleName());
                    Thread.sleep(Math.min(5_000, failures * 250L));
                }
            }
            throw new IllegalStateException("CDC consumer retry deadline exceeded");
        }
    }

    @Command(name = "plan", description = "Discover and persist a reviewed repair plan")
    static final class Plan extends DatabaseCommand implements Callable<Integer> {
        @Option(names = "--bad-config", defaultValue = "bad-fees-v2") String badConfig;
        @Option(names = "--target-fee-bps", defaultValue = "290") int targetFeeBps;
        @Option(names = "--partitions", defaultValue = "32") int partitions;
        @Option(names = "--dry-run", defaultValue = "false") boolean dryRun;

        @Override public Integer call() throws Exception {
            var planner = new RepairPlanner(dataSource());
            long started = System.nanoTime();
            var preview = planner.preview(badConfig, targetFeeBps, partitions);
            UUID jobId = planner.persist(preview, dryRun);
            long millis = (System.nanoTime() - started) / 1_000_000;
            System.out.printf("JOB_ID=%s%nAFFECTED=%d%nPLAN_HASH=%s%nPLAN_MILLIS=%d%n",
                    jobId, preview.items().size(), preview.planHash(), millis);
            return 0;
        }
    }

    @Command(name = "worker", description = "Run a restart-safe repair worker")
    static final class Worker extends DatabaseCommand implements Callable<Integer> {
        @Option(names = "--job", required = true) UUID jobId;
        @Option(names = "--threads", defaultValue = "1") int threads;
        @Option(names = "--stale-after-ms", defaultValue = "2000") long staleAfterMs;
        @Option(names = "--delay-after-claim-ms", defaultValue = "0") long delayAfterClaimMs;
        @Option(names = "--ready-file", defaultValue = "/tmp/repair-worker-ready") Path readyFile;

        @Override public Integer call() throws Exception {
            Files.writeString(readyFile, "ready\n");
            var engine = new RepairEngine(dataSource());
            int databaseFailures = 0;
            while (true) {
                try {
                    engine.recoverAbandonedClaims(jobId, Duration.ofMillis(staleAfterMs));
                    var result = engine.run(jobId, threads, ignored -> false, false,
                            Duration.ofMillis(delayAfterClaimMs));
                    long unfinished = engine.unfinishedItems(jobId);
                    System.out.printf(
                            "WORKER_APPLIED=%d%nWORKER_CONFLICTS=%d%nWORKER_UNFINISHED=%d%n",
                            result.applied(), result.conflicts(), unfinished);
                    if (unfinished == 0) return 0;
                    Thread.sleep(Math.max(100, staleAfterMs));
                } catch (Exception transientFailure) {
                    databaseFailures++;
                    System.err.printf("WORKER_RETRY=%d cause=%s%n", databaseFailures,
                            transientFailure.getClass().getSimpleName());
                    Thread.sleep(Math.min(5_000, 250L * databaseFailures));
                }
            }
        }
    }

    @Command(name = "validate", description = "Validate final payment and lineage invariants")
    static final class Validate extends DatabaseCommand implements Callable<Integer> {
        @Override public Integer call() throws Exception {
            var result = new InvariantValidator(dataSource()).validateAll();
            System.out.printf(
                    "VALID=%s%nINVALID_PAYMENTS=%d%nUNBALANCED_LEDGERS=%d%nDUPLICATE_LINEAGE=%d%n",
                    result.valid(), result.invalidPayments(), result.unbalancedLedgers(),
                    result.duplicateLineage());
            return result.valid() ? 0 : 1;
        }
    }

    @Command(name = "scale-experiment",
            description = "Run the 100,000+ record controlled scale experiment")
    static final class Scale implements Callable<Integer> {
        @Option(names = "--jdbc") String jdbc =
                env("JDBC_URL", "jdbc:postgresql://localhost:54329/repairs");
        @Option(names = "--user") String user = env("DB_USER", "repairs");
        @Option(names = "--password") String password = env("DB_PASSWORD", "repairs");
        @Option(names = "--kafka") String kafka =
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        @Option(names = "--records", defaultValue = "100000") int records;
        @Option(names = "--workers", defaultValue = "8") int workers;
        @Option(names = "--report",
                defaultValue = "build/reports/repair-scale-experiment.json")
        Path report;

        @Override public Integer call() throws Exception {
            var result = ScaleExperiment.run(jdbc, user, password, kafka, records, workers);
            ScaleExperiment.writeReport(result, report);
            System.out.println("Scale experiment passed; report=" + report.toAbsolutePath());
            return 0;
        }

        private static String env(String name, String fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : value;
        }
    }

    @Override public void run() {
        CommandLine.usage(this, System.out);
    }

    @Command(name = "experiment", description = "Run the controlled end-to-end experiment")
    static final class Experiment implements Callable<Integer> {
        @Option(names = "--jdbc") String jdbc =
                env("JDBC_URL", "jdbc:postgresql://localhost:54329/repairs");
        @Option(names = "--user") String user = env("DB_USER", "repairs");
        @Option(names = "--password") String password = env("DB_PASSWORD", "repairs");
        @Option(names = "--kafka") String kafka =
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        @Option(names = "--records", defaultValue = "1000") int records;
        @Option(names = "--workers", defaultValue = "8") int workers;
        @Option(names = "--report", defaultValue = "build/reports/repair-experiment.json")
        Path report;

        @Override public Integer call() throws Exception {
            var result = EndToEndExperiment.run(jdbc, user, password, kafka, records, workers);
            EndToEndExperiment.writeReport(result, report);
            System.out.println("Experiment passed; report=" + report.toAbsolutePath());
            return 0;
        }

        private static String env(String name, String fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : value;
        }
    }

    private static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

package dev.datarepair.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import dev.datarepair.cdc.LineageConsumer;
import dev.datarepair.cdc.OutboxPublisher;
import dev.datarepair.db.Database;
import dev.datarepair.repair.InvariantValidator;
import dev.datarepair.repair.RepairEngine;
import dev.datarepair.repair.RepairPlanner;
import dev.datarepair.workload.PaymentWorkload;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The recruiting-scale controlled experiment. It intentionally remains
 * separate from the original baseline experiment and writes a new artifact.
 */
public final class ScaleExperiment {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private ScaleExperiment() {}

    public static Map<String, Object> run(String jdbcUrl, String user, String password,
                                          String kafka, int records, int workers) throws Exception {
        DataSource ds = Database.dataSource(jdbcUrl, user, password);
        Database.initialize(ds);
        var workload = new PaymentWorkload(ds);

        long seedStarted = System.nanoTime();
        workload.generateCapturesBatched(Math.max(1_000, records / 10), 1_000, 71,
                "payments-3.0.0", "fees-good-v3", 290);
        List<UUID> corruptedIds = workload.generateCapturesBatched(records, 1_000, 73,
                "payments-2.9.0", "bad-fees-v2", 790);
        long seedNanos = System.nanoTime() - seedStarted;

        String groupId = "lineage-scale-" + UUID.randomUUID();
        int initiallyPublished;
        try (var publisher = new OutboxPublisher(ds, kafka)) {
            initiallyPublished = publisher.drain();
            publisher.replayPublished(Math.min(1_000, initiallyPublished));
        }
        int initiallyLineaged;
        try (var consumer = new LineageConsumer(ds, kafka, groupId)) {
            initiallyLineaged = consumer.consumeUntilIdle(
                    Duration.ofMillis(500), Duration.ofMinutes(10));
        }

        var baselineLatencies = Collections.synchronizedList(new ArrayList<Long>());
        for (int i = 0; i < 200; i++) {
            long started = System.nanoTime();
            workload.capture(UUID.randomUUID(), UUID.randomUUID(), 250_000,
                    "payments-3.0.0", "ordinary-writes-v1", 290);
            baselineLatencies.add(System.nanoTime() - started);
        }

        var concurrentLatencies = Collections.synchronizedList(new ArrayList<Long>());
        var writerErrors = new AtomicInteger();
        var writesSucceeded = new AtomicInteger();
        var writerRunning = new AtomicBoolean(true);
        Thread writer = new Thread(() -> {
            int operation = 0;
            while (writerRunning.get()) {
                long started = System.nanoTime();
                try {
                    if (operation % 50 == 0) {
                        UUID target = corruptedIds.get(
                                Math.floorMod(operation / 50, corruptedIds.size()));
                        if (workload.refund(target, 1, "payments-3.0.0",
                                "ordinary-refund-v1")) {
                            writesSucceeded.incrementAndGet();
                        }
                    } else {
                        workload.capture(UUID.randomUUID(), UUID.randomUUID(), 250_000,
                                "payments-3.0.0", "ordinary-writes-v1", 290);
                        writesSucceeded.incrementAndGet();
                    }
                    concurrentLatencies.add(System.nanoTime() - started);
                } catch (Exception e) {
                    writerErrors.incrementAndGet();
                }
                operation++;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "ordinary-payment-writer");
        writer.start();

        var planner = new RepairPlanner(ds);
        long discoveryStarted = System.nanoTime();
        var preview = planner.preview("bad-fees-v2", 290, Math.max(32, workers * 4));
        long discoveryNanos = System.nanoTime() - discoveryStarted;
        long versionsBeforeDryRun = scalar(ds, "SELECT COALESCE(sum(row_version),0) FROM payments");
        UUID dryJob = planner.persist(preview, true);
        boolean dryRunRejected = false;
        try {
            new RepairEngine(ds).run(dryJob, workers, ignored -> false, false);
        } catch (IllegalStateException expected) {
            dryRunRejected = true;
        }
        long versionsAfterDryRun = scalar(ds, "SELECT COALESCE(sum(row_version),0) FROM payments");
        long dryRunRepairMutations = scalar(ds, """
                SELECT count(*) FROM audit_log
                WHERE job_id=? AND action IN ('ITEM_APPLIED','AUTOMATIC_ROLLBACK')
                """, dryJob);

        long planPersistStarted = System.nanoTime();
        UUID jobId = planner.persist(preview, false);
        long planPersistNanos = System.nanoTime() - planPersistStarted;
        UUID rollbackTarget = preview.items().get(0).before().paymentId();
        workload.refund(preview.items().get(1).before().paymentId(), 1,
                "payments-3.0.0", "ordinary-refund-v1");

        var engine = new RepairEngine(ds);
        var initialRun = engine.run(jobId, workers, rollbackTarget::equals, true);
        writerRunning.set(false);
        writer.join(Duration.ofSeconds(30).toMillis());

        Instant recoveryStarted = Instant.now();
        int claimsRecovered = engine.recoverAbandonedClaims(jobId, Duration.ZERO);
        var recoveryRun = engine.run(jobId, workers, ignored -> false, false);
        long recoveryMillis = Duration.between(recoveryStarted, Instant.now()).toMillis();

        var followupPreview = planner.preview("bad-fees-v2", 290,
                Math.max(32, workers * 4));
        UUID followupJob = planner.persist(followupPreview, false);
        var followupRun = engine.run(followupJob, workers, ignored -> false, false);

        int finallyPublished;
        try (var publisher = new OutboxPublisher(ds, kafka)) {
            finallyPublished = publisher.drain();
            publisher.replayPublished(Math.min(1_000, initiallyPublished + finallyPublished));
        }
        int finallyLineaged;
        try (var consumer = new LineageConsumer(ds, kafka, groupId)) {
            finallyLineaged = consumer.consumeUntilIdle(
                    Duration.ofMillis(500), Duration.ofMinutes(10));
        }

        var validation = new InvariantValidator(ds).validateAll();
        long totalApplied = initialRun.applied() + recoveryRun.applied() + followupRun.applied();
        long repairNanos = initialRun.elapsed().toNanos() + recoveryRun.elapsed().toNanos()
                + followupRun.elapsed().toNanos();
        long remainingWrongFees = scalar(ds, "SELECT count(*) FROM payments WHERE fee_bps<>290");
        long retries = scalar(ds, """
                SELECT COALESCE(sum(GREATEST(attempt_count-1,0)),0) FROM repair_items
                WHERE NOT dry_run_job(job_id)
                """);
        long duplicateApplies = scalar(ds, """
                SELECT count(*) FROM (
                  SELECT payment_id FROM audit_log WHERE action='ITEM_APPLIED'
                  GROUP BY payment_id HAVING count(*)>1
                ) duplicated
                """);
        long duplicateLineage = scalar(ds, """
                SELECT count(*) FROM (
                  SELECT event_id FROM record_lineage GROUP BY event_id HAVING count(*)>1
                ) duplicated
                """);

        var report = new LinkedHashMap<String, Object>();
        report.put("generatedAt", Instant.now().toString());
        report.put("environment", Map.of(
                "java", System.getProperty("java.version"),
                "os", System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                "kafka", kafka, "postgres", jdbcUrl));
        report.put("input", Map.of(
                "corruptedRecords", records, "workers", workers,
                "badFeeBps", 790, "targetFeeBps", 290));
        report.put("seed", Map.of(
                "elapsedMillis", seedNanos / 1_000_000,
                "recordsPerSecond", records / seconds(seedNanos)));
        report.put("cdc", Map.of(
                "initiallyPublished", initiallyPublished,
                "initiallyLineaged", initiallyLineaged,
                "finallyPublished", finallyPublished,
                "finallyLineaged", finallyLineaged,
                "duplicatesRedelivered", Math.min(2_000,
                        (initiallyPublished + finallyPublished) * 2),
                "duplicateLineageRows", duplicateLineage));
        report.put("discovery", Map.of(
                "affectedRecords", preview.items().size(),
                "elapsedMillis", discoveryNanos / 1_000_000,
                "recordsPerSecond", preview.items().size() / seconds(discoveryNanos),
                "planHash", preview.planHash(),
                "planPersistMillis", planPersistNanos / 1_000_000));
        report.put("dryRun", Map.of(
                "mutationRejected", dryRunRejected,
                "repairMutationAuditEvents", dryRunRepairMutations,
                "globalRowVersionChangedByOrdinaryWrites",
                versionsBeforeDryRun != versionsAfterDryRun));
        var writeMetrics = new LinkedHashMap<String, Object>();
        double baselineP50 = percentileMillis(baselineLatencies, 0.50);
        double baselineP95 = percentileMillis(baselineLatencies, 0.95);
        double concurrentP50 = percentileMillis(concurrentLatencies, 0.50);
        double concurrentP95 = percentileMillis(concurrentLatencies, 0.95);
        writeMetrics.put("baselineSamples", baselineLatencies.size());
        writeMetrics.put("concurrentSamples", concurrentLatencies.size());
        writeMetrics.put("successfulWrites", writesSucceeded.get());
        writeMetrics.put("errors", writerErrors.get());
        writeMetrics.put("baselineP50Millis", baselineP50);
        writeMetrics.put("baselineP95Millis", baselineP95);
        writeMetrics.put("concurrentP50Millis", concurrentP50);
        writeMetrics.put("concurrentP95Millis", concurrentP95);
        writeMetrics.put("p50ImpactPercent", percentChange(baselineP50, concurrentP50));
        writeMetrics.put("p95ImpactPercent", percentChange(baselineP95, concurrentP95));
        report.put("ordinaryWrites", writeMetrics);
        var repairMetrics = new LinkedHashMap<String, Object>();
        repairMetrics.put("uniqueCorruptedRecords", records);
        repairMetrics.put("successfulApplies", totalApplied);
        repairMetrics.put("elapsedMillis", repairNanos / 1_000_000);
        repairMetrics.put("recordsPerSecond", totalApplied / seconds(repairNanos));
        repairMetrics.put("conflicts", initialRun.conflicts() + recoveryRun.conflicts());
        repairMetrics.put("retries", retries);
        repairMetrics.put("followupAffected", followupPreview.items().size());
        repairMetrics.put("automaticRollbacks", initialRun.rolledBack());
        repairMetrics.put("claimsRecovered", claimsRecovered);
        repairMetrics.put("recoveryMillis", recoveryMillis);
        report.put("repair", repairMetrics);
        report.put("correctness", Map.of(
                "remainingWrongFees", remainingWrongFees,
                "invalidPayments", validation.invalidPayments(),
                "unbalancedLedgers", validation.unbalancedLedgers(),
                "duplicateLineage", duplicateLineage,
                "duplicateRepairApplies", duplicateApplies,
                "valid", validation.valid()));

        boolean passed = preview.items().size() == records
                && dryRunRejected && dryRunRepairMutations == 0
                && concurrentLatencies.size() >= 100 && writesSucceeded.get() >= 100
                && claimsRecovered == 1 && initialRun.rolledBack() == 1
                && initialRun.conflicts() >= 1
                && totalApplied == records && remainingWrongFees == 0
                && duplicateLineage == 0 && duplicateApplies == 0 && validation.valid();
        report.put("passed", passed);
        if (!passed) {
            throw new IllegalStateException("Scale experiment failed: "
                    + JSON.writeValueAsString(report));
        }
        return report;
    }

    public static void writeReport(Map<String, Object> report, Path path) throws Exception {
        Files.createDirectories(path.getParent());
        JSON.writeValue(path.toFile(), report);
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private static double percentileMillis(List<Long> values, double percentile) {
        if (values.isEmpty()) return 0;
        var sorted = new ArrayList<>(values);
        sorted.sort(Long::compare);
        int index = Math.min(sorted.size() - 1,
                Math.max(0, (int) Math.ceil(percentile * sorted.size()) - 1));
        return sorted.get(index) / 1_000_000.0;
    }

    private static double percentChange(double baseline, double observed) {
        return baseline == 0 ? 0 : ((observed - baseline) / baseline) * 100.0;
    }

    private static long scalar(DataSource ds, String sql, Object... values) throws Exception {
        try (var c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) ps.setObject(i + 1, values[i]);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}

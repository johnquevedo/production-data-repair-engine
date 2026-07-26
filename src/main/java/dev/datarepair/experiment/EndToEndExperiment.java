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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public final class EndToEndExperiment {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private EndToEndExperiment() {}

    public static Map<String, Object> run(String jdbcUrl, String user, String password,
                                          String kafka, int records, int workers) throws Exception {
        DataSource ds = Database.dataSource(jdbcUrl, user, password);
        Database.initialize(ds);
        var workload = new PaymentWorkload(ds);
        int goodCount = Math.max(10, records / 10);
        workload.generateCaptures(goodCount, 7, "payments-2.4.0", "fees-good-v2", 290);
        workload.generateCaptures(records, 11, "payments-2.3.1", "bad-fees-v1", 790);

        int published;
        try (var publisher = new OutboxPublisher(ds, kafka)) {
            published = publisher.drain();
            publisher.replayPublished(Math.min(100, published));
        }
        int lineageInserted;
        try (var consumer = new LineageConsumer(ds, kafka, "lineage-experiment-" + UUID.randomUUID())) {
            lineageInserted = consumer.consumeUntilIdle(Duration.ofMillis(300), Duration.ofSeconds(30));
        }

        var planner = new RepairPlanner(ds);
        long versionSumBefore = scalar(ds, "SELECT COALESCE(sum(row_version),0) FROM payments");
        var preview = planner.preview("bad-fees-v1", 290, 16);
        UUID dryRunJob = planner.persist(preview, true);
        boolean dryRunRejected = false;
        try {
            new RepairEngine(ds).run(dryRunJob, workers, id -> false, false);
        } catch (IllegalStateException expected) {
            dryRunRejected = true;
        }
        long versionSumAfter = scalar(ds, "SELECT COALESCE(sum(row_version),0) FROM payments");

        UUID firstJob = planner.persist(preview, false);
        UUID rollbackTarget = preview.items().get(0).before().paymentId();
        UUID conflictTarget = preview.items().get(1).before().paymentId();
        workload.refund(conflictTarget, 1, "payments-2.4.1", "concurrent-refund-v1");

        var engine = new RepairEngine(ds);
        var firstRun = engine.run(firstJob, workers, rollbackTarget::equals, true);
        boolean rollbackRestoredBusinessState = paymentBusinessStateMatches(
                ds, preview.items().get(0).before());
        Instant recoveryStarted = Instant.now();
        int recoveredClaims = engine.recoverAbandonedClaims(firstJob, Duration.ZERO);
        var recoveredRun = engine.run(firstJob, workers, id -> false, false);
        long recoveryMillis = Duration.between(recoveryStarted, Instant.now()).toMillis();

        var followupPreview = planner.preview("bad-fees-v1", 290, 16);
        UUID followupJob = planner.persist(followupPreview, false);
        var finalRun = engine.run(followupJob, workers, id -> false, false);
        var validation = new InvariantValidator(ds).validateAll();

        long applied = firstRun.applied() + recoveredRun.applied() + finalRun.applied();
        long elapsedNanos = firstRun.elapsed().toNanos() + recoveredRun.elapsed().toNanos()
                + finalRun.elapsed().toNanos();
        double throughput = applied / (elapsedNanos / 1_000_000_000.0);
        long remainingWrongFees = scalar(ds, "SELECT count(*) FROM payments WHERE fee_bps<>290");
        long auditEvents = scalar(ds, "SELECT count(*) FROM audit_log");
        long duplicateLineage = scalar(ds, """
                SELECT count(*) FROM (SELECT event_id FROM record_lineage GROUP BY event_id HAVING count(*)>1) d
                """);

        var report = new LinkedHashMap<String, Object>();
        report.put("generatedAt", Instant.now().toString());
        report.put("environment", Map.of("java", System.getProperty("java.version"),
                "os", System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                "kafka", kafka, "postgres", jdbcUrl));
        report.put("input", Map.of("corruptedRecords", records, "controlRecords", goodCount,
                "workers", workers, "badFeeBps", 790, "targetFeeBps", 290));
        report.put("cdc", Map.of("outboxEventsPublished", published,
                "uniqueLineageEventsInserted", lineageInserted,
                "duplicateReplayRequested", Math.min(100, published),
                "duplicateLineageRows", duplicateLineage));
        report.put("dryRun", Map.of("affectedRecords", preview.items().size(),
                "planHash", preview.planHash(), "mutationRejected", dryRunRejected,
                "rowVersionSumUnchanged", versionSumBefore == versionSumAfter));
        report.put("faultInjection", Map.of(
                "abandonedClaimInjected", firstRun.abandonedClaim(),
                "claimsRecovered", recoveredClaims,
                "recoveryMillis", recoveryMillis,
                "automaticRollbacks", firstRun.rolledBack(),
                "rollbackRestoredBusinessState", rollbackRestoredBusinessState,
                "concurrentWriteConflicts", firstRun.conflicts()));
        report.put("repair", Map.of("repairAttemptsApplied", applied,
                "uniqueCorruptedRecords", records,
                "elapsedMillis", elapsedNanos / 1_000_000,
                "recordsPerSecond", throughput,
                "followupAffected", followupPreview.items().size(),
                "remainingWrongFees", remainingWrongFees));
        report.put("correctness", Map.of("valid", validation.valid(),
                "invalidPayments", validation.invalidPayments(),
                "unbalancedLedgers", validation.unbalancedLedgers(),
                "duplicateLineage", validation.duplicateLineage(),
                "auditEvents", auditEvents));
        boolean passed = published == records + goodCount
                && lineageInserted == published
                && duplicateLineage == 0
                && dryRunRejected && versionSumBefore == versionSumAfter
                && firstRun.abandonedClaim() && recoveredClaims == 1
                && firstRun.rolledBack() == 1 && rollbackRestoredBusinessState
                && firstRun.conflicts() == 1
                && remainingWrongFees == 0 && validation.valid();
        report.put("passed", passed);
        if (!passed) throw new IllegalStateException("Experiment failed: " + JSON.writeValueAsString(report));
        return report;
    }

    public static void writeReport(Map<String, Object> report, Path path) throws Exception {
        Files.createDirectories(path.getParent());
        JSON.writeValue(path.toFile(), report);
    }

    private static long scalar(DataSource ds, String sql) throws Exception {
        try (var c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static boolean paymentBusinessStateMatches(
            DataSource ds, dev.datarepair.model.PaymentSnapshot expected) throws Exception {
        try (var c = ds.getConnection(); var ps = c.prepareStatement("""
                SELECT fee_bps,fee_cents,merchant_net_cents,captured_cents,refunded_cents
                FROM payments WHERE payment_id=?
                """)) {
            ps.setObject(1, expected.paymentId());
            try (var rs = ps.executeQuery()) {
                return rs.next()
                        && rs.getInt(1) == expected.feeBps()
                        && rs.getLong(2) == expected.feeCents()
                        && rs.getLong(3) == expected.merchantNetCents()
                        && rs.getLong(4) == expected.capturedCents()
                        && rs.getLong(5) == expected.refundedCents();
            }
        }
    }
}

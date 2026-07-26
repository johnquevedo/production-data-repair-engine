package dev.datarepair.repair;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.datarepair.db.Database;
import dev.datarepair.model.PaymentSnapshot;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class RepairEngine {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final DataSource dataSource;

    public RepairEngine(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public RunResult run(UUID jobId, int workers, Predicate<UUID> injectValidationFailure,
                         boolean abandonOneClaim) throws Exception {
        return run(jobId, workers, injectValidationFailure, abandonOneClaim, Duration.ZERO);
    }

    public RunResult run(UUID jobId, int workers, Predicate<UUID> injectValidationFailure,
                         boolean abandonOneClaim, Duration delayAfterClaim) throws Exception {
        ensureRunnable(jobId);
        var abandoned = new AtomicBoolean(false);
        var applied = new AtomicInteger();
        var conflicts = new AtomicInteger();
        var rolledBack = new AtomicInteger();
        long started = System.nanoTime();
        var executor = Executors.newFixedThreadPool(workers);
        try {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < workers; i++) {
                String worker = "worker-" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
                futures.add(executor.submit(() -> {
                    try {
                        while (true) {
                            ClaimedItem item = claim(jobId, worker);
                            if (item == null) break;
                            if (abandonOneClaim && abandoned.compareAndSet(false, true)) {
                                continue; // fault injection: process dies after durable claim
                            }
                            if (!delayAfterClaim.isZero()) Thread.sleep(delayAfterClaim.toMillis());
                            Outcome outcome = apply(item, worker,
                                    injectValidationFailure.test(item.paymentId()));
                            switch (outcome) {
                                case APPLIED -> applied.incrementAndGet();
                                case CONFLICT -> conflicts.incrementAndGet();
                                case ROLLED_BACK -> rolledBack.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (var future : futures) future.get();
        } finally {
            executor.shutdownNow();
        }
        var states = stateCounts(jobId);
        String status;
        if (states.getOrDefault("PENDING", 0L) > 0 || states.getOrDefault("APPLYING", 0L) > 0) {
            status = "FAILED";
        } else if (states.getOrDefault("CONFLICT", 0L) > 0) {
            status = "COMPLETED_WITH_CONFLICTS";
        } else if (states.getOrDefault("ROLLED_BACK", 0L) > 0) {
            status = "ROLLED_BACK";
        } else {
            status = "COMPLETED";
        }
        updateJobStatus(jobId, status);
        return new RunResult(applied.get(), conflicts.get(), rolledBack.get(),
                abandoned.get(), Duration.ofNanos(System.nanoTime() - started), states);
    }

    public int recoverAbandonedClaims(UUID jobId, Duration olderThan) throws Exception {
        return Database.transaction(dataSource, connection -> {
            int reset;
            try (PreparedStatement ps = connection.prepareStatement("""
                    UPDATE repair_items SET state='PENDING',worker_id=NULL,claimed_at=NULL,
                      error='recovered abandoned claim'
                    WHERE job_id=? AND state='APPLYING' AND claimed_at < clock_timestamp()-(? * interval '1 millisecond')
                    """)) {
                ps.setObject(1, jobId);
                ps.setLong(2, olderThan.toMillis());
                reset = ps.executeUpdate();
            }
            if (reset > 0) {
                RepairPlanner.audit(connection, jobId, null, "CRASH_RECOVERY", null,
                        JSON.writeValueAsString(Map.of("claimsReset", reset,
                                "olderThanMs", olderThan.toMillis())));
            }
            return reset;
        });
    }

    public long unfinishedItems(UUID jobId) throws Exception {
        try (var connection = dataSource.getConnection();
             var ps = connection.prepareStatement("""
                     SELECT count(*) FROM repair_items
                     WHERE job_id=? AND state IN ('PENDING','APPLYING')
                     """)) {
            ps.setObject(1, jobId);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void ensureRunnable(UUID jobId) throws Exception {
        Database.transaction(dataSource, connection -> {
            try (PreparedStatement check = connection.prepareStatement(
                    "SELECT dry_run,status FROM repair_jobs WHERE job_id=? FOR UPDATE")) {
                check.setObject(1, jobId);
                try (var rs = check.executeQuery()) {
                    if (!rs.next()) throw new IllegalArgumentException("Unknown job " + jobId);
                    if (rs.getBoolean(1)) throw new IllegalStateException("Dry-run plans cannot mutate data");
                }
            }
            try (PreparedStatement ps = connection.prepareStatement("""
                    UPDATE repair_jobs SET status='RUNNING',started_at=COALESCE(started_at,clock_timestamp()),
                      completed_at=NULL WHERE job_id=?
                    """)) {
                ps.setObject(1, jobId);
                ps.executeUpdate();
            }
            RepairPlanner.audit(connection, jobId, null, "JOB_STARTED", null, "{}");
            return null;
        });
    }

    private ClaimedItem claim(UUID jobId, String worker) throws Exception {
        return Database.transaction(dataSource, connection -> {
            UUID paymentId;
            try (PreparedStatement select = connection.prepareStatement("""
                    SELECT payment_id FROM repair_items
                    WHERE job_id=? AND state='PENDING'
                    ORDER BY partition_id,payment_id FOR UPDATE SKIP LOCKED LIMIT 1
                    """)) {
                select.setObject(1, jobId);
                try (var rs = select.executeQuery()) {
                    if (!rs.next()) return null;
                    paymentId = rs.getObject(1, UUID.class);
                }
            }
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE repair_items SET state='APPLYING',worker_id=?,claimed_at=clock_timestamp(),
                      attempt_count=attempt_count+1 WHERE job_id=? AND payment_id=?
                    RETURNING expected_row_version,before_image::text,after_image::text,partition_id
                    """)) {
                update.setString(1, worker);
                update.setObject(2, jobId);
                update.setObject(3, paymentId);
                try (var rs = update.executeQuery()) {
                    rs.next();
                    var item = new ClaimedItem(jobId, paymentId, rs.getLong(1),
                            JSON.readValue(rs.getString(2), PaymentSnapshot.class),
                            JSON.readValue(rs.getString(3), PaymentSnapshot.class), rs.getInt(4));
                    RepairPlanner.audit(connection, jobId, paymentId, "ITEM_CLAIMED", worker, "{}");
                    return item;
                }
            }
        });
    }

    private Outcome apply(ClaimedItem item, String worker, boolean injectFailure) throws Exception {
        return Database.transaction(dataSource, connection -> {
            PaymentSnapshot target = item.after();
            long fee = injectFailure ? target.feeCents() + 1 : target.feeCents();
            long net = target.capturedCents() - target.refundedCents() - fee;
            int updated;
            try (PreparedStatement ps = connection.prepareStatement("""
                    UPDATE payments SET fee_bps=?,fee_cents=?,merchant_net_cents=?,
                      row_version=row_version+1,updated_at=clock_timestamp()
                    WHERE payment_id=? AND row_version=?
                    """)) {
                ps.setInt(1, target.feeBps());
                ps.setLong(2, fee);
                ps.setLong(3, net);
                ps.setObject(4, item.paymentId());
                ps.setLong(5, item.expectedVersion());
                updated = ps.executeUpdate();
            }
            if (updated == 0) {
                finishItem(connection, item, worker, "CONFLICT", "row version changed");
                RepairPlanner.audit(connection, item.jobId(), item.paymentId(), "CONCURRENT_WRITE_CONFLICT",
                        worker, JSON.writeValueAsString(Map.of("expectedVersion", item.expectedVersion())));
                return Outcome.CONFLICT;
            }

            long delta = fee - item.before().feeCents();
            insertLedger(connection, item, "PLATFORM_FEE", delta, "REPAIR");
            insertLedger(connection, item, "MERCHANT", -delta, "REPAIR");
            boolean valid = validatePayment(connection, item.paymentId(), target.feeBps(), target.feeCents());
            if (!valid) {
                int restored;
                try (PreparedStatement rollback = connection.prepareStatement("""
                        UPDATE payments SET fee_bps=?,fee_cents=?,merchant_net_cents=?,
                          row_version=row_version+1,updated_at=clock_timestamp()
                        WHERE payment_id=? AND row_version=?
                        """)) {
                    rollback.setInt(1, item.before().feeBps());
                    rollback.setLong(2, item.before().feeCents());
                    rollback.setLong(3, item.before().merchantNetCents());
                    rollback.setObject(4, item.paymentId());
                    rollback.setLong(5, item.expectedVersion() + 1);
                    restored = rollback.executeUpdate();
                }
                if (restored != 1) throw new IllegalStateException("Rollback lost concurrent-write race");
                insertLedger(connection, item, "PLATFORM_FEE", -delta, "ROLLBACK");
                insertLedger(connection, item, "MERCHANT", delta, "ROLLBACK");
                finishItem(connection, item, worker, "ROLLED_BACK", "postcondition failed");
                RepairPlanner.audit(connection, item.jobId(), item.paymentId(), "AUTOMATIC_ROLLBACK",
                        worker, JSON.writeValueAsString(Map.of("injected", injectFailure)));
                checkpoint(connection, item);
                return Outcome.ROLLED_BACK;
            }
            finishItem(connection, item, worker, "APPLIED", null);
            checkpoint(connection, item);
            RepairPlanner.audit(connection, item.jobId(), item.paymentId(), "ITEM_APPLIED", worker,
                    JSON.writeValueAsString(Map.of("oldFeeCents", item.before().feeCents(),
                            "newFeeCents", target.feeCents())));
            return Outcome.APPLIED;
        });
    }

    private static boolean validatePayment(Connection connection, UUID paymentId,
                                           int expectedBps, long expectedFee) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("""
                SELECT p.fee_bps,p.fee_cents,
                  p.merchant_net_cents=p.captured_cents-p.refunded_cents-p.fee_cents,
                  COALESCE((SELECT sum(amount_cents) FROM ledger_entries l WHERE l.payment_id=p.payment_id),0)
                FROM payments p WHERE p.payment_id=?
                """)) {
            ps.setObject(1, paymentId);
            try (var rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) == expectedBps && rs.getLong(2) == expectedFee
                        && rs.getBoolean(3) && rs.getLong(4) == 0;
            }
        }
    }

    private static void insertLedger(Connection c, ClaimedItem item, String account,
                                     long amount, String kind) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO ledger_entries(entry_id,payment_id,account,amount_cents,entry_kind,repair_job_id)
                VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING
                """)) {
            ps.setObject(1, UUID.randomUUID());
            ps.setObject(2, item.paymentId());
            ps.setString(3, account);
            ps.setLong(4, amount);
            ps.setString(5, kind);
            ps.setObject(6, item.jobId());
            ps.executeUpdate();
        }
    }

    private static void finishItem(Connection c, ClaimedItem item, String worker,
                                   String state, String error) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE repair_items SET state=?,completed_at=clock_timestamp(),error=?
                WHERE job_id=? AND payment_id=? AND state='APPLYING' AND worker_id=?
                """)) {
            ps.setString(1, state);
            ps.setString(2, error);
            ps.setObject(3, item.jobId());
            ps.setObject(4, item.paymentId());
            ps.setString(5, worker);
            if (ps.executeUpdate() != 1) throw new IllegalStateException("Lost item claim");
        }
    }

    private static void checkpoint(Connection c, ClaimedItem item) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO repair_checkpoints(job_id,partition_id,completed_count,last_payment_id)
                VALUES (?,?,1,?) ON CONFLICT(job_id,partition_id) DO UPDATE
                SET completed_count=repair_checkpoints.completed_count+1,last_payment_id=excluded.last_payment_id,
                    updated_at=clock_timestamp()
                """)) {
            ps.setObject(1, item.jobId());
            ps.setInt(2, item.partition());
            ps.setObject(3, item.paymentId());
            ps.executeUpdate();
        }
    }

    private Map<String, Long> stateCounts(UUID jobId) throws Exception {
        try (var c = dataSource.getConnection();
             var ps = c.prepareStatement("SELECT state,count(*) FROM repair_items WHERE job_id=? GROUP BY state")) {
            ps.setObject(1, jobId);
            var result = new java.util.HashMap<String, Long>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) result.put(rs.getString(1), rs.getLong(2));
            }
            return Map.copyOf(result);
        }
    }

    private void updateJobStatus(UUID jobId, String status) throws Exception {
        Database.transaction(dataSource, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                    UPDATE repair_jobs SET status=?,completed_at=clock_timestamp() WHERE job_id=?
                    """)) {
                ps.setString(1, status);
                ps.setObject(2, jobId);
                ps.executeUpdate();
            }
            RepairPlanner.audit(connection, jobId, null, "JOB_" + status, null, "{}");
            return null;
        });
    }

    private record ClaimedItem(UUID jobId, UUID paymentId, long expectedVersion,
                               PaymentSnapshot before, PaymentSnapshot after, int partition) {}
    private enum Outcome { APPLIED, CONFLICT, ROLLED_BACK }
    public record RunResult(int applied, int conflicts, int rolledBack, boolean abandonedClaim,
                            Duration elapsed, Map<String, Long> states) {}
}

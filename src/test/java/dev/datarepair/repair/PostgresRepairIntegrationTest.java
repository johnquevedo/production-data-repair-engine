package dev.datarepair.repair;

import dev.datarepair.IntegrationEnvironment;
import dev.datarepair.workload.PaymentWorkload;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class PostgresRepairIntegrationTest {
    @Test
    void dryRunCrashRecoveryConflictRollbackAndIdempotency() throws Exception {
        try (var environment = IntegrationEnvironment.create()) {
            var ds = environment.dataSource();
            var workload = new PaymentWorkload(ds);
            workload.generateCaptures(12, 19, "payments-2.3.1", "bad-fees-v1", 790);
            seedLineageFromOutbox(ds);

            var planner = new RepairPlanner(ds);
            var preview = planner.preview("bad-fees-v1", 290, 4);
            assertEquals(12, preview.items().size());
            UUID dry = planner.persist(preview, true);
            assertThrows(IllegalStateException.class,
                    () -> new RepairEngine(ds).run(dry, 2, id -> false, false));

            UUID rollbackTarget = preview.items().get(0).before().paymentId();
            UUID conflictTarget = preview.items().get(1).before().paymentId();
            UUID job = planner.persist(preview, false);
            assertTrue(workload.refund(conflictTarget, 1, "concurrent", "refund-v1"));

            var engine = new RepairEngine(ds);
            var first = engine.run(job, 3, rollbackTarget::equals, false);
            assertEquals(1, first.rolledBack());
            assertEquals(1, first.conflicts());

            var followup = planner.preview("bad-fees-v1", 290, 4);
            UUID followupJob = planner.persist(followup, false);
            var interrupted = engine.run(followupJob, 3, id -> false, true);
            assertTrue(interrupted.abandonedClaim());
            assertEquals(1, interrupted.applied());
            assertEquals(1, engine.recoverAbandonedClaims(followupJob, Duration.ZERO));
            var finalRun = engine.run(followupJob, 3, id -> false, false);
            assertEquals(1, finalRun.applied());
            assertTrue(new InvariantValidator(ds).validateAll().valid());

            var repeated = engine.run(followupJob, 3, id -> false, false);
            assertEquals(0, repeated.applied());
            assertEquals(0, scalar(ds, """
                    SELECT count(*) FROM (
                      SELECT payment_id FROM audit_log WHERE action='ITEM_APPLIED'
                      GROUP BY payment_id HAVING count(*) > 1
                    ) duplicate_applies
                    """));
            assertEquals(12, scalar(ds, """
                    SELECT count(*) FROM repair_items
                    WHERE job_id=? AND state IN ('APPLIED','CONFLICT','ROLLED_BACK')
                    """, job));
            assertTrue(scalar(ds, """
                    SELECT COALESCE(sum(completed_count),0) FROM repair_checkpoints
                    WHERE job_id=?
                    """, job) >= 11);
            assertThrows(java.sql.SQLException.class, () -> {
                try (var c = ds.getConnection();
                     var ps = c.prepareStatement("DELETE FROM audit_log")) {
                    ps.executeUpdate();
                }
            });
        }
    }

    private static long scalar(javax.sql.DataSource ds, String sql, Object... values)
            throws Exception {
        try (var c = ds.getConnection(); var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) ps.setObject(i + 1, values[i]);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void seedLineageFromOutbox(javax.sql.DataSource ds) throws Exception {
        try (var c = ds.getConnection(); var ps = c.prepareStatement("""
                INSERT INTO record_lineage(event_id,record_id,event_type,source_txid,code_version,
                  config_version,payload,kafka_partition,kafka_offset)
                SELECT event_id,aggregate_id,event_type,source_txid,code_version,config_version,payload,0,sequence_id
                FROM outbox_events
                """)) {
            ps.executeUpdate();
        }
    }
}

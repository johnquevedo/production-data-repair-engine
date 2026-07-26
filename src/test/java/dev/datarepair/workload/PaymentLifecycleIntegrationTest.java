package dev.datarepair.workload;

import dev.datarepair.IntegrationEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class PaymentLifecycleIntegrationTest {
    @Test
    void authorizationCaptureAndRefundPreserveLedgerAndLineageMetadata() throws Exception {
        try (var environment = IntegrationEnvironment.create()) {
            var ds = environment.dataSource();
            var workload = new PaymentWorkload(ds);
            UUID paymentId = UUID.randomUUID();

            workload.authorize(paymentId, UUID.randomUUID(), 100_00,
                    "payments-3.0.0", "auth-rules-v2");
            assertEquals("AUTHORIZED", text(ds,
                    "SELECT status FROM payments WHERE payment_id='" + paymentId + "'"));
            assertEquals(0, number(ds,
                    "SELECT count(*) FROM ledger_entries WHERE payment_id='" + paymentId + "'"));

            assertTrue(workload.captureAuthorized(paymentId,
                    "payments-3.0.1", "bad-fees-v2", 790));
            assertFalse(workload.captureAuthorized(paymentId,
                    "payments-3.0.1", "bad-fees-v2", 790));
            assertTrue(workload.refund(paymentId, 2_500,
                    "payments-3.0.2", "refund-rules-v1"));
            assertFalse(workload.refund(paymentId, 8_000,
                    "payments-3.0.2", "refund-rules-v1"));

            assertEquals("PARTIALLY_REFUNDED", text(ds,
                    "SELECT status FROM payments WHERE payment_id='" + paymentId + "'"));
            assertEquals(0, number(ds, """
                    SELECT COALESCE(sum(amount_cents),0) FROM ledger_entries
                    WHERE payment_id='%s'
                    """.formatted(paymentId)));
            assertEquals(3, number(ds,
                    "SELECT count(*) FROM outbox_events WHERE aggregate_id='" + paymentId + "'"));
            assertEquals(1, number(ds, """
                    SELECT count(*) FROM outbox_events
                    WHERE aggregate_id='%s' AND event_type='PAYMENT_AUTHORIZED'
                      AND code_version='payments-3.0.0' AND config_version='auth-rules-v2'
                    """.formatted(paymentId)));
        }
    }

    private static long number(javax.sql.DataSource ds, String sql) throws Exception {
        try (var c = ds.getConnection(); var ps = c.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String text(javax.sql.DataSource ds, String sql) throws Exception {
        try (var c = ds.getConnection(); var ps = c.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            rs.next();
            return rs.getString(1);
        }
    }
}

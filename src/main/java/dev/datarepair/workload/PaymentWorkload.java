package dev.datarepair.workload;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.datarepair.db.Database;
import dev.datarepair.model.PaymentSnapshot;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * A ledger-backed card payment workload. Captures are written atomically with
 * balanced customer/merchant/platform entries and a transactional outbox event.
 */
public final class PaymentWorkload {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final DataSource dataSource;

    public PaymentWorkload(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void generateCaptures(int count, long seed, String codeVersion,
                                 String configVersion, int feeBps) throws SQLException {
        var random = new SplittableRandom(seed);
        for (int i = 0; i < count; i++) {
            long amount = 1_000 + random.nextLong(499_001);
            capture(UUID.randomUUID(), UUID.randomUUID(), amount, codeVersion, configVersion, feeBps);
        }
    }

    /**
     * High-volume equivalent of {@link #generateCaptures}; each chunk commits
     * payments, balanced ledger rows, and outbox envelopes atomically.
     */
    public List<UUID> generateCapturesBatched(int count, int chunkSize, long seed,
                                              String codeVersion, String configVersion,
                                              int feeBps) throws SQLException {
        var random = new SplittableRandom(seed);
        var ids = new ArrayList<UUID>(count);
        for (int offset = 0; offset < count; offset += chunkSize) {
            int current = Math.min(chunkSize, count - offset);
            var chunk = new ArrayList<SeedCapture>(current);
            for (int i = 0; i < current; i++) {
                var capture = new SeedCapture(UUID.randomUUID(), UUID.randomUUID(),
                        100_000 + random.nextLong(400_001));
                chunk.add(capture);
                ids.add(capture.paymentId());
            }
            Database.transaction(dataSource, connection -> {
                try (PreparedStatement payment = connection.prepareStatement("""
                        INSERT INTO payments(payment_id,merchant_id,amount_cents,fee_bps,fee_cents,
                          merchant_net_cents,status,captured_cents,code_version,config_version)
                        VALUES (?,?,?,?,?,?,'CAPTURED',?,?,?)
                        """);
                     PreparedStatement ledger = connection.prepareStatement("""
                        INSERT INTO ledger_entries(entry_id,payment_id,account,amount_cents,entry_kind)
                        VALUES (?,?,?,?,'CAPTURE')
                        """);
                     PreparedStatement outbox = connection.prepareStatement("""
                        INSERT INTO outbox_events(event_id,aggregate_id,event_type,code_version,
                          config_version,payload) VALUES (?,?,'PAYMENT_CAPTURED',?,?,?::jsonb)
                        """)) {
                    for (SeedCapture seedCapture : chunk) {
                        long fee = PaymentSnapshot.feeFor(seedCapture.amountCents(), 0, feeBps);
                        long net = seedCapture.amountCents() - fee;
                        payment.setObject(1, seedCapture.paymentId());
                        payment.setObject(2, seedCapture.merchantId());
                        payment.setLong(3, seedCapture.amountCents());
                        payment.setInt(4, feeBps);
                        payment.setLong(5, fee);
                        payment.setLong(6, net);
                        payment.setLong(7, seedCapture.amountCents());
                        payment.setString(8, codeVersion);
                        payment.setString(9, configVersion);
                        payment.addBatch();

                        addLedgerBatch(ledger, seedCapture.paymentId(), "CUSTOMER",
                                -seedCapture.amountCents());
                        addLedgerBatch(ledger, seedCapture.paymentId(), "MERCHANT", net);
                        addLedgerBatch(ledger, seedCapture.paymentId(), "PLATFORM_FEE", fee);

                        outbox.setObject(1, UUID.randomUUID());
                        outbox.setObject(2, seedCapture.paymentId());
                        outbox.setString(3, codeVersion);
                        outbox.setString(4, configVersion);
                        outbox.setString(5, JSON.writeValueAsString(Map.of(
                                "paymentId", seedCapture.paymentId(),
                                "amountCents", seedCapture.amountCents(),
                                "feeBps", feeBps, "feeCents", fee,
                                "merchantNetCents", net)));
                        outbox.addBatch();
                    }
                    payment.executeBatch();
                    ledger.executeBatch();
                    outbox.executeBatch();
                }
                return null;
            });
        }
        return List.copyOf(ids);
    }

    public UUID authorize(UUID paymentId, UUID merchantId, long amountCents,
                          String codeVersion, String configVersion) throws SQLException {
        return Database.transaction(dataSource, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO payments(payment_id,merchant_id,amount_cents,fee_bps,fee_cents,
                      merchant_net_cents,status,captured_cents,code_version,config_version)
                    VALUES (?,?,?,0,0,0,'AUTHORIZED',0,?,?)
                    """)) {
                ps.setObject(1, paymentId);
                ps.setObject(2, merchantId);
                ps.setLong(3, amountCents);
                ps.setString(4, codeVersion);
                ps.setString(5, configVersion);
                ps.executeUpdate();
            }
            insertOutbox(connection, paymentId, "PAYMENT_AUTHORIZED", codeVersion, configVersion,
                    Map.of("paymentId", paymentId, "amountCents", amountCents));
            return paymentId;
        });
    }

    public boolean captureAuthorized(UUID paymentId, String codeVersion,
                                     String configVersion, int feeBps) throws SQLException {
        return Database.transaction(dataSource, connection -> {
            long amount;
            try (PreparedStatement lock = connection.prepareStatement("""
                    SELECT amount_cents FROM payments
                    WHERE payment_id=? AND status='AUTHORIZED' FOR UPDATE
                    """)) {
                lock.setObject(1, paymentId);
                try (var rs = lock.executeQuery()) {
                    if (!rs.next()) return false;
                    amount = rs.getLong(1);
                }
            }
            long fee = PaymentSnapshot.feeFor(amount, 0, feeBps);
            long net = amount - fee;
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE payments SET fee_bps=?,fee_cents=?,merchant_net_cents=?,
                      status='CAPTURED',captured_cents=?,code_version=?,config_version=?,
                      row_version=row_version+1,updated_at=clock_timestamp()
                    WHERE payment_id=?
                    """)) {
                update.setInt(1, feeBps);
                update.setLong(2, fee);
                update.setLong(3, net);
                update.setLong(4, amount);
                update.setString(5, codeVersion);
                update.setString(6, configVersion);
                update.setObject(7, paymentId);
                update.executeUpdate();
            }
            insertLedger(connection, paymentId, "CUSTOMER", -amount, "CAPTURE", null);
            insertLedger(connection, paymentId, "MERCHANT", net, "CAPTURE", null);
            insertLedger(connection, paymentId, "PLATFORM_FEE", fee, "CAPTURE", null);
            insertOutbox(connection, paymentId, "PAYMENT_CAPTURED", codeVersion, configVersion,
                    Map.of("paymentId", paymentId, "amountCents", amount,
                            "feeBps", feeBps, "feeCents", fee, "merchantNetCents", net));
            return true;
        });
    }

    public UUID capture(UUID paymentId, UUID merchantId, long amountCents,
                        String codeVersion, String configVersion, int feeBps) throws SQLException {
        return Database.transaction(dataSource, connection -> {
            long fee = PaymentSnapshot.feeFor(amountCents, 0, feeBps);
            long net = amountCents - fee;
            try (PreparedStatement ps = connection.prepareStatement("""
                    INSERT INTO payments(payment_id, merchant_id, amount_cents, fee_bps, fee_cents,
                      merchant_net_cents, status, captured_cents, code_version, config_version)
                    VALUES (?, ?, ?, ?, ?, ?, 'CAPTURED', ?, ?, ?)
                    """)) {
                ps.setObject(1, paymentId);
                ps.setObject(2, merchantId);
                ps.setLong(3, amountCents);
                ps.setInt(4, feeBps);
                ps.setLong(5, fee);
                ps.setLong(6, net);
                ps.setLong(7, amountCents);
                ps.setString(8, codeVersion);
                ps.setString(9, configVersion);
                ps.executeUpdate();
            }
            insertLedger(connection, paymentId, "CUSTOMER", -amountCents, "CAPTURE", null);
            insertLedger(connection, paymentId, "MERCHANT", net, "CAPTURE", null);
            insertLedger(connection, paymentId, "PLATFORM_FEE", fee, "CAPTURE", null);
            insertOutbox(connection, paymentId, "PAYMENT_CAPTURED", codeVersion, configVersion,
                    Map.of("paymentId", paymentId, "amountCents", amountCents,
                            "feeBps", feeBps, "feeCents", fee, "merchantNetCents", net));
            return paymentId;
        });
    }

    public boolean refund(UUID paymentId, long refundCents, String codeVersion,
                          String configVersion) throws SQLException {
        return Database.transaction(dataSource, connection -> {
            try (PreparedStatement lock = connection.prepareStatement(
                    "SELECT captured_cents, refunded_cents, fee_bps FROM payments WHERE payment_id=? FOR UPDATE")) {
                lock.setObject(1, paymentId);
                try (var rs = lock.executeQuery()) {
                    if (!rs.next()) return false;
                    long captured = rs.getLong(1);
                    long previousRefunded = rs.getLong(2);
                    int bps = rs.getInt(3);
                    if (refundCents <= 0 || previousRefunded + refundCents > captured) return false;
                    long refunded = previousRefunded + refundCents;
                    long oldFee = PaymentSnapshot.feeFor(captured, previousRefunded, bps);
                    long newFee = PaymentSnapshot.feeFor(captured, refunded, bps);
                    long feeDelta = oldFee - newFee;
                    String status = refunded == captured ? "REFUNDED" : "PARTIALLY_REFUNDED";
                    try (PreparedStatement update = connection.prepareStatement("""
                            UPDATE payments SET refunded_cents=?, fee_cents=?, merchant_net_cents=?,
                              status=?, code_version=?, config_version=?, row_version=row_version+1,
                              updated_at=clock_timestamp() WHERE payment_id=?
                            """)) {
                        update.setLong(1, refunded);
                        update.setLong(2, newFee);
                        update.setLong(3, captured - refunded - newFee);
                        update.setString(4, status);
                        update.setString(5, codeVersion);
                        update.setString(6, configVersion);
                        update.setObject(7, paymentId);
                        update.executeUpdate();
                    }
                    insertLedger(connection, paymentId, "CUSTOMER", refundCents, "REFUND", null);
                    insertLedger(connection, paymentId, "MERCHANT", -refundCents + feeDelta, "REFUND", null);
                    insertLedger(connection, paymentId, "PLATFORM_FEE", -feeDelta, "REFUND", null);
                    insertOutbox(connection, paymentId, "PAYMENT_REFUNDED", codeVersion, configVersion,
                            Map.of("paymentId", paymentId, "refundCents", refundCents,
                                    "totalRefundedCents", refunded, "feeCents", newFee));
                    return true;
                }
            }
        });
    }

    private static void insertLedger(Connection c, UUID paymentId, String account, long amount,
                                     String kind, UUID repairJobId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO ledger_entries(entry_id,payment_id,account,amount_cents,entry_kind,repair_job_id)
                VALUES (?,?,?,?,?,?)
                """)) {
            ps.setObject(1, UUID.randomUUID());
            ps.setObject(2, paymentId);
            ps.setString(3, account);
            ps.setLong(4, amount);
            ps.setString(5, kind);
            ps.setObject(6, repairJobId);
            ps.executeUpdate();
        }
    }

    private static void addLedgerBatch(PreparedStatement ledger, UUID paymentId,
                                       String account, long amount) throws SQLException {
        ledger.setObject(1, UUID.randomUUID());
        ledger.setObject(2, paymentId);
        ledger.setString(3, account);
        ledger.setLong(4, amount);
        ledger.addBatch();
    }

    private static void insertOutbox(Connection c, UUID paymentId, String type, String code,
                                     String config, Map<String, ?> payload) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO outbox_events(event_id,aggregate_id,event_type,code_version,config_version,payload)
                VALUES (?,?,?,?,?,?::jsonb)
                """)) {
            ps.setObject(1, UUID.randomUUID());
            ps.setObject(2, paymentId);
            ps.setString(3, type);
            ps.setString(4, code);
            ps.setString(5, config);
            ps.setString(6, JSON.writeValueAsString(payload));
            ps.executeUpdate();
        }
    }

    private record SeedCapture(UUID paymentId, UUID merchantId, long amountCents) {}
}

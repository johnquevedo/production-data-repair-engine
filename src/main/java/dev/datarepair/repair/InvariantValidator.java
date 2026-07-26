package dev.datarepair.repair;

import javax.sql.DataSource;
import java.sql.PreparedStatement;

public final class InvariantValidator {
    private final DataSource dataSource;

    public InvariantValidator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public ValidationResult validateAll() throws Exception {
        try (var connection = dataSource.getConnection()) {
            long invalidPayments = scalar(connection.prepareStatement("""
                    SELECT count(*) FROM payments WHERE
                      merchant_net_cents<>captured_cents-refunded_cents-fee_cents
                      OR captured_cents<0 OR captured_cents>amount_cents
                      OR refunded_cents<0 OR refunded_cents>captured_cents
                    """));
            long unbalancedLedgers = scalar(connection.prepareStatement("""
                    SELECT count(*) FROM (
                      SELECT payment_id FROM ledger_entries GROUP BY payment_id HAVING sum(amount_cents)<>0
                    ) broken
                    """));
            long duplicateLineage = scalar(connection.prepareStatement("""
                    SELECT count(*) FROM (
                      SELECT event_id FROM record_lineage GROUP BY event_id HAVING count(*)>1
                    ) duplicates
                    """));
            return new ValidationResult(invalidPayments == 0 && unbalancedLedgers == 0
                    && duplicateLineage == 0, invalidPayments, unbalancedLedgers, duplicateLineage);
        }
    }

    private static long scalar(PreparedStatement ps) throws Exception {
        try (ps; var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public record ValidationResult(boolean valid, long invalidPayments,
                                   long unbalancedLedgers, long duplicateLineage) {}
}

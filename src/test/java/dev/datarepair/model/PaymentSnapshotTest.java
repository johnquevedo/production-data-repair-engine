package dev.datarepair.model;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentSnapshotTest {
    @Test
    void computesRoundedFeeAndConservesMerchantNet() {
        var before = new PaymentSnapshot(UUID.randomUUID(), 10_001, 790, 790,
                10_001, 0, 9_211, "CAPTURED", "code", "bad", 3);
        var repaired = before.withFee(290);

        assertEquals(290, repaired.feeCents());
        assertEquals(9_711, repaired.merchantNetCents());
        assertEquals(repaired.capturedCents() - repaired.refundedCents() - repaired.feeCents(),
                repaired.merchantNetCents());
    }

    @Test
    void feeIsBasedOnNetCapturedAfterRefund() {
        assertEquals(218, PaymentSnapshot.feeFor(10_000, 2_500, 290));
    }

    @Test
    void fullyRefundedPaymentHasNoFeeOrMerchantNet() {
        var captured = new PaymentSnapshot(UUID.randomUUID(), 25_000, 790, 1_975,
                25_000, 25_000, -1_975, "REFUNDED", "code", "bad", 4);
        var repaired = captured.withFee(290);

        assertEquals(0, repaired.feeCents());
        assertEquals(0, repaired.merchantNetCents());
    }

    @Test
    void rejectsRefundGreaterThanCaptureInFeeCalculation() {
        assertThrows(IllegalArgumentException.class,
                () -> PaymentSnapshot.feeFor(100, 101, 290));
    }
}

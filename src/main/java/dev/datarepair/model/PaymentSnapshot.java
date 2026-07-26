package dev.datarepair.model;

import java.util.UUID;

public record PaymentSnapshot(
        UUID paymentId,
        long amountCents,
        int feeBps,
        long feeCents,
        long capturedCents,
        long refundedCents,
        long merchantNetCents,
        String status,
        String codeVersion,
        String configVersion,
        long rowVersion) {

    public static long feeFor(long capturedCents, long refundedCents, int feeBps) {
        if (capturedCents < 0 || refundedCents < 0 || refundedCents > capturedCents) {
            throw new IllegalArgumentException("refund must be between zero and captured amount");
        }
        if (feeBps < 0 || feeBps > 10_000) {
            throw new IllegalArgumentException("fee basis points must be between 0 and 10000");
        }
        return Math.floorDiv((capturedCents - refundedCents) * feeBps + 5_000L, 10_000L);
    }

    public PaymentSnapshot withFee(int targetFeeBps) {
        long targetFee = feeFor(capturedCents, refundedCents, targetFeeBps);
        return new PaymentSnapshot(paymentId, amountCents, targetFeeBps, targetFee,
                capturedCents, refundedCents, capturedCents - refundedCents - targetFee,
                status, codeVersion, configVersion, rowVersion);
    }
}

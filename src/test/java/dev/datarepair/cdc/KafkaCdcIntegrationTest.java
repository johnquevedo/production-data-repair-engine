package dev.datarepair.cdc;

import dev.datarepair.IntegrationEnvironment;
import dev.datarepair.workload.PaymentWorkload;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
class KafkaCdcIntegrationTest {
    @Test
    void duplicateKafkaDeliveryCreatesOneLineageFact() throws Exception {
        try (var environment = IntegrationEnvironment.create()) {
            var ds = environment.dataSource();
            String topic = "payment-cdc-it-" + UUID.randomUUID();
            new PaymentWorkload(ds).generateCaptures(20, 99, "code-v1", "config-v1", 290);

            try (var publisher = new OutboxPublisher(ds, IntegrationEnvironment.kafka(), topic)) {
                assertEquals(20, publisher.drain());
                publisher.replayPublished(20);
            }
            try (var consumer = new LineageConsumer(ds, IntegrationEnvironment.kafka(),
                    "test-" + UUID.randomUUID(), topic)) {
                assertEquals(20, consumer.consumeUntilIdle(Duration.ofMillis(200), Duration.ofSeconds(20)));
            }
            try (var c = ds.getConnection();
                 var ps = c.prepareStatement("""
                         SELECT count(*),count(DISTINCT event_id),
                           count(*) FILTER (WHERE code_version='code-v1'
                             AND config_version='config-v1'
                             AND source_txid > 0 AND kafka_offset >= 0)
                         FROM record_lineage
                         """);
                 var rs = ps.executeQuery()) {
                rs.next();
                assertEquals(20, rs.getLong(1));
                assertEquals(20, rs.getLong(2));
                assertEquals(20, rs.getLong(3));
            }
        }
    }
}

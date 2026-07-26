package dev.datarepair.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.datarepair.db.Database;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class OutboxPublisher implements AutoCloseable {
    public static final String TOPIC = "payment-cdc";
    private final DataSource dataSource;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private static final ObjectMapper JSON = new ObjectMapper();

    public OutboxPublisher(DataSource dataSource, String bootstrapServers) {
        this(dataSource, bootstrapServers, TOPIC);
    }

    public OutboxPublisher(DataSource dataSource, String bootstrapServers, String topic) {
        this.dataSource = dataSource;
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(props);
    }

    public int publishBatch(int limit) throws Exception {
        return Database.transaction(dataSource, connection -> {
            var events = new ArrayList<Envelope>();
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT sequence_id,event_id,aggregate_id,event_type,source_txid,code_version,
                           config_version,payload::text,created_at::text
                    FROM outbox_events WHERE published_at IS NULL ORDER BY sequence_id
                    FOR UPDATE SKIP LOCKED LIMIT ?
                    """)) {
                ps.setInt(1, limit);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        events.add(new Envelope(rs.getLong(1), rs.getObject(2, UUID.class),
                                rs.getObject(3, UUID.class), rs.getString(4), rs.getLong(5),
                                rs.getString(6), rs.getString(7), JSON.readTree(rs.getString(8)),
                                rs.getString(9)));
                    }
                }
            }
            var sends = new ArrayList<Future<RecordMetadata>>(events.size());
            for (Envelope event : events) sends.add(producer.send(new ProducerRecord<>(
                    topic, event.aggregateId().toString(), JSON.writeValueAsString(event))));
            for (var send : sends) send.get();
            if (!events.isEmpty()) {
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE outbox_events SET published_at=clock_timestamp() WHERE sequence_id=?")) {
                    for (Envelope event : events) {
                        update.setLong(1, event.sequenceId());
                        update.addBatch();
                    }
                    update.executeBatch();
                }
            }
            return events.size();
        });
    }

    public int drain() throws Exception {
        return drain(5_000, Duration.ZERO);
    }

    public int drain(int batchSize, Duration delayBetweenBatches) throws Exception {
        int total = 0;
        int current;
        do {
            current = publishBatch(batchSize);
            total += current;
            if (current > 0 && !delayBetweenBatches.isZero()) {
                Thread.sleep(delayBetweenBatches.toMillis());
            }
        } while (current > 0);
        producer.flush();
        return total;
    }

    public void replayPublished(int limit) throws Exception {
        List<String[]> records = Database.transaction(dataSource, connection -> {
            var found = new ArrayList<String[]>();
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT aggregate_id::text,
                      json_build_object('sequenceId',sequence_id,'eventId',event_id,'aggregateId',aggregate_id,
                      'eventType',event_type,'sourceTxid',source_txid,'codeVersion',code_version,
                      'configVersion',config_version,'payload',payload,'createdAt',created_at)::text
                    FROM outbox_events WHERE published_at IS NOT NULL ORDER BY sequence_id LIMIT ?
                    """)) {
                ps.setInt(1, limit);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) found.add(new String[]{rs.getString(1), rs.getString(2)});
                }
            }
            return found;
        });
        for (var record : records) producer.send(new ProducerRecord<>(topic, record[0], record[1])).get();
        producer.flush();
    }

    @Override public void close() {
        producer.close(Duration.ofSeconds(5));
    }

    public record Envelope(long sequenceId, UUID eventId, UUID aggregateId, String eventType,
                           long sourceTxid, String codeVersion, String configVersion,
                           Object payload, String createdAt) {}
}

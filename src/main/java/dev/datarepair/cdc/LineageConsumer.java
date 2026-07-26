package dev.datarepair.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.datarepair.db.Database;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public final class LineageConsumer implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final DataSource dataSource;
    private final KafkaConsumer<String, String> consumer;

    public LineageConsumer(DataSource dataSource, String bootstrapServers, String groupId) {
        this(dataSource, bootstrapServers, groupId, OutboxPublisher.TOPIC);
    }

    public LineageConsumer(DataSource dataSource, String bootstrapServers, String groupId, String topic) {
        this.dataSource = dataSource;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
    }

    public int consumeUntilIdle(Duration idle, Duration maximum) throws Exception {
        long deadline = System.nanoTime() + maximum.toNanos();
        int inserted = 0;
        int idlePolls = 0;
        while (System.nanoTime() < deadline && idlePolls < 3) {
            var records = consumer.poll(idle);
            if (records.isEmpty()) {
                idlePolls++;
                continue;
            }
            idlePolls = 0;
            int batchInserted = Database.transaction(dataSource, connection -> {
                try (PreparedStatement ps = connection.prepareStatement("""
                            INSERT INTO record_lineage(event_id,record_id,event_type,source_txid,
                              code_version,config_version,payload,kafka_partition,kafka_offset)
                            VALUES (?,?,?,?,?,?,?::jsonb,?,?) ON CONFLICT(event_id) DO NOTHING
                            """)) {
                    for (var record : records) {
                        JsonNode envelope = JSON.readTree(record.value());
                        ps.setObject(1, UUID.fromString(envelope.get("eventId").asText()));
                        ps.setObject(2, UUID.fromString(envelope.get("aggregateId").asText()));
                        ps.setString(3, envelope.get("eventType").asText());
                        ps.setLong(4, envelope.get("sourceTxid").asLong());
                        ps.setString(5, envelope.get("codeVersion").asText());
                        ps.setString(6, envelope.get("configVersion").asText());
                        ps.setString(7, envelope.get("payload").toString());
                        ps.setInt(8, record.partition());
                        ps.setLong(9, record.offset());
                        ps.addBatch();
                    }
                    int count = 0;
                    for (int result : ps.executeBatch()) {
                        if (result > 0 || result == java.sql.Statement.SUCCESS_NO_INFO) count++;
                    }
                    return count;
                }
            });
            inserted += batchInserted;
            consumer.commitSync();
        }
        return inserted;
    }

    @Override public void close() {
        consumer.close(Duration.ofSeconds(5));
    }
}

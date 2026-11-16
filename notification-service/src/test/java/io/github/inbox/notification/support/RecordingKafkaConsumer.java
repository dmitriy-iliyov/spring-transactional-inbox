package io.github.inbox.notification.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Reads a topic from its beginning and keeps every record it has seen, so a test can look for the record it caused
 * among those other tests left on the same topic.
 */
public final class RecordingKafkaConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new CopyOnWriteArrayList<>();

    public RecordingKafkaConsumer(String bootstrapServers, String topic) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ), new StringDeserializer(), new StringDeserializer());
        this.consumer.subscribe(List.of(topic));
    }

    /**
     * Polls once and returns the record whose header {@code header} has the value {@code value}, if seen so far.
     */
    public Optional<ConsumerRecord<String, String>> findByHeader(String header, String value) {
        consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        return seen.stream()
                .filter(record -> value.equals(header(record, header)))
                .findFirst();
    }

    /**
     * Polls once and returns the record whose JSON value has {@code field} equal to {@code value}, if seen so far.
     */
    public Optional<ConsumerRecord<String, String>> findByPayloadField(String field, Object value) {
        consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        String needle = "\"%s\":%s".formatted(field, value);
        return seen.stream()
                .filter(record -> record.value() != null && record.value().contains(needle))
                .findFirst();
    }

    public static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        consumer.close();
    }
}

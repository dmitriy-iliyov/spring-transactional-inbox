package io.github.inbox.loyalty.logging;

import io.github.dmitriyiliyov.oncebox.core.OnceboxHeaders;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import org.slf4j.MDC;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.github.inbox.contract.ObservabilityVocabulary.CUSTOMER_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_TYPE;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_OFFSET;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_PARTITION;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_TOPIC;
import static io.github.inbox.contract.ObservabilityVocabulary.ORDER_ID;

/**
 * The MDC of one consumed record: its event id and type, its Kafka coordinates and the order and customer it is
 * about, carried by every line logged while it is handled - by the listener, the inbox and the points service alike.
 * <p>
 * Headers are copied as they arrived, so a record with a malformed event id is still logged under the value it
 * carried. A field the record does not have is left out rather than written as {@code null}. Closing the context
 * removes exactly the keys it put, so the next record on the listener thread starts clean and keys set by others
 * stay.
 */
public final class ConsumedEventLogContext implements AutoCloseable {

    private final List<String> keys = new ArrayList<>();

    private ConsumedEventLogContext() {
    }

    public static ConsumedEventLogContext open(Message<?> message) {
        ConsumedEventLogContext context = new ConsumedEventLogContext();
        MessageHeaders headers = message.getHeaders();
        context.put(EVENT_ID, headers.get(OnceboxHeaders.EVENT_ID.getValue()));
        context.put(EVENT_TYPE, headers.get(OnceboxHeaders.EVENT_TYPE.getValue()));
        context.put(KAFKA_TOPIC, headers.get(KafkaHeaders.RECEIVED_TOPIC));
        context.put(KAFKA_PARTITION, headers.get(KafkaHeaders.RECEIVED_PARTITION));
        context.put(KAFKA_OFFSET, headers.get(KafkaHeaders.OFFSET));
        switch (message.getPayload()) {
            case OrderCreatedEvent created -> context.putOrder(created.orderId(), created.customerId());
            case OrderCancelledEvent cancelled -> context.putOrder(cancelled.orderId(), cancelled.customerId());
            default -> {
            }
        }
        return context;
    }

    @Override
    public void close() {
        keys.forEach(MDC::remove);
    }

    private void putOrder(Long orderId, Long customerId) {
        put(ORDER_ID, orderId);
        put(CUSTOMER_ID, customerId);
    }

    private void put(String key, Object value) {
        if (value == null) {
            return;
        }
        String text = value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : value.toString();
        MDC.put(key, text);
        keys.add(key);
    }
}

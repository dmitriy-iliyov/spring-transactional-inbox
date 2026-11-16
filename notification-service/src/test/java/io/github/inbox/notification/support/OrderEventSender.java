package io.github.inbox.notification.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.inbox.contract.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Sends an order event the way the order service's outbox does: the JSON payload with the event id, type and
 * payload type in headers. Sending the same id again is how a test stages a redelivery; records sent under one key
 * share a partition, so they are consumed in the order they were sent.
 */
public final class OrderEventSender {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OrderEventSender(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void send(UUID eventId, String eventType, Object payload) {
        send(eventId, eventType, null, payload);
    }

    /**
     * Sends the payload with its event type but without an event id header, as a producer outside the outbox might.
     */
    public void sendWithoutEventId(String eventType, Object payload) {
        sendWithoutEventId(eventType, null, payload);
    }

    public void sendWithoutEventId(String eventType, String key, Object payload) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Topics.ORDER_EVENTS, key, objectMapper.writeValueAsString(payload));
            record.headers().add("oncebox_event_type", eventType.getBytes(StandardCharsets.UTF_8));
            record.headers().add("oncebox_event_payload_type",
                    payload.getClass().getName().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not send event", e);
        }
    }

    public void send(UUID eventId, String eventType, String key, Object payload) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Topics.ORDER_EVENTS, key, objectMapper.writeValueAsString(payload));
            record.headers().add("oncebox_event_id", eventId.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("oncebox_event_type", eventType.getBytes(StandardCharsets.UTF_8));
            record.headers().add("oncebox_event_payload_type",
                    payload.getClass().getName().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not send event " + eventId, e);
        }
    }
}

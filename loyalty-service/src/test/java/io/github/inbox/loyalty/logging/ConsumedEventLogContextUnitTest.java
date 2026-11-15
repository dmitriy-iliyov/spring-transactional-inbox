package io.github.inbox.loyalty.logging;

import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static io.github.inbox.contract.ObservabilityVocabulary.CORRELATION_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.CUSTOMER_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_TYPE;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_OFFSET;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_PARTITION;
import static io.github.inbox.contract.ObservabilityVocabulary.KAFKA_TOPIC;
import static io.github.inbox.contract.ObservabilityVocabulary.ORDER_ID;
import static org.assertj.core.api.Assertions.assertThat;

class ConsumedEventLogContextUnitTest {

    private static final String EVENT_ID_VALUE = "0195f0a0-0000-7000-8000-000000000001";
    private static final Instant NOW = Instant.parse("2026-11-15T10:00:00Z");

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    @DisplayName("UT open() when a full order-created record arrives should put its ids and Kafka coordinates")
    void open_whenOrderCreatedRecord_shouldPutIdsAndCoordinates() {
        // given
        Message<?> message = record(created())
                .setHeader("oncebox_event_type", "order-created")
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "orders.events")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 3)
                .setHeader(KafkaHeaders.OFFSET, 41L)
                .build();

        // when
        try (ConsumedEventLogContext ignored = ConsumedEventLogContext.open(message)) {

            // then
            assertThat(MDC.get(EVENT_ID)).isEqualTo(EVENT_ID_VALUE);
            assertThat(MDC.get(EVENT_TYPE)).isEqualTo("order-created");
            assertThat(MDC.get(KAFKA_TOPIC)).isEqualTo("orders.events");
            assertThat(MDC.get(KAFKA_PARTITION)).isEqualTo("3");
            assertThat(MDC.get(KAFKA_OFFSET)).isEqualTo("41");
            assertThat(MDC.get(ORDER_ID)).isEqualTo("7");
            assertThat(MDC.get(CUSTOMER_ID)).isEqualTo("42");
        }
    }

    @Test
    @DisplayName("UT open() when an order-cancelled record arrives should put its order and customer")
    void open_whenOrderCancelledRecord_shouldPutOrderAndCustomer() {
        // given
        Message<?> message = record(new OrderCancelledEvent(8L, 43L, null, NOW)).build();

        // when
        try (ConsumedEventLogContext ignored = ConsumedEventLogContext.open(message)) {

            // then
            assertThat(MDC.get(ORDER_ID)).isEqualTo("8");
            assertThat(MDC.get(CUSTOMER_ID)).isEqualTo("43");
        }
    }

    @Test
    @DisplayName("UT open() when the event id header arrives as raw bytes should log it as text")
    void open_whenHeaderIsBytes_shouldPutItAsText() {
        // given
        Message<?> message = MessageBuilder.withPayload(created())
                .setHeader("oncebox_event_id", EVENT_ID_VALUE.getBytes(StandardCharsets.UTF_8))
                .build();

        // when
        try (ConsumedEventLogContext ignored = ConsumedEventLogContext.open(message)) {

            // then
            assertThat(MDC.get(EVENT_ID)).isEqualTo(EVENT_ID_VALUE);
        }
    }

    @Test
    @DisplayName("UT open() when the record lacks headers and has an unknown payload should put nothing for them")
    void open_whenHeadersAndOrderAreMissing_shouldLeaveFieldsOut() {
        // given
        Message<?> message = MessageBuilder.withPayload("something else").build();

        // when
        try (ConsumedEventLogContext ignored = ConsumedEventLogContext.open(message)) {

            // then
            assertThat(MDC.get(EVENT_ID)).isNull();
            assertThat(MDC.get(KAFKA_TOPIC)).isNull();
            assertThat(MDC.get(ORDER_ID)).isNull();
        }
    }

    @Test
    @DisplayName("UT close() when the record is handled should remove its keys and keep the ones set by others")
    void close_whenRecordIsHandled_shouldRemoveOnlyItsKeys() {
        // given
        MDC.put(CORRELATION_ID, "kept");
        ConsumedEventLogContext context = ConsumedEventLogContext.open(record(created()).build());

        // when
        context.close();

        // then
        assertThat(MDC.get(EVENT_ID)).isNull();
        assertThat(MDC.get(ORDER_ID)).isNull();
        assertThat(MDC.get(CORRELATION_ID)).isEqualTo("kept");
    }

    private static MessageBuilder<?> record(Object payload) {
        return MessageBuilder.withPayload(payload).setHeader("oncebox_event_id", EVENT_ID_VALUE);
    }

    private static OrderCreatedEvent created() {
        return new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
    }
}

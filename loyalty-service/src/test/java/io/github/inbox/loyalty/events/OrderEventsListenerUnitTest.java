package io.github.inbox.loyalty.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import io.github.inbox.loyalty.points.PointsService;
import io.github.inbox.loyalty.support.CapturedLogs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderEventsListenerUnitTest {

    private static final UUID EVENT_ID = UUID.fromString("0195f0a0-0000-7000-8000-000000000001");
    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    @Mock
    private PointsService points;

    private InMemoryIdempotentConsumer inbox;
    private OrderEventsListener tested;

    @BeforeEach
    void setUp() {
        inbox = new InMemoryIdempotentConsumer();
        tested = new OrderEventsListener(inbox, points);
    }

    @Test
    @DisplayName("UT onOrderEvent() when order-created arrives should accrue points under the event id of its header")
    void onOrderEvent_whenOrderCreated_shouldAccrueUnderHeaderEventId() {
        // given
        OrderCreatedEvent event = new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);

        // when
        tested.onOrderEvent(message(event));

        // then
        verify(points).accrue(event);
        assertThat(inbox.consumedIds()).containsExactly(EVENT_ID);
    }

    @Test
    @DisplayName("UT onOrderEvent() when order-cancelled arrives should revert the points")
    void onOrderEvent_whenOrderCancelled_shouldRevertPoints() {
        // given
        OrderCancelledEvent event = new OrderCancelledEvent(7L, 42L, null, NOW);

        // when
        tested.onOrderEvent(message(event));

        // then
        verify(points).revert(event);
    }

    @Test
    @DisplayName("UT onOrderEvent() when the payload is of no known event should refuse it untouched")
    void onOrderEvent_whenPayloadIsUnknown_shouldThrowUnsupportedEventException() {
        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(message("something else")))
                .isInstanceOf(UnsupportedEventException.class)
                .hasMessage("No handling for event payload java.lang.String");
        verifyNoInteractions(points);
    }

    @Test
    @DisplayName("UT onOrderEvent() when the message has no event id header should refuse it as malformed")
    void onOrderEvent_whenEventIdHeaderIsMissing_shouldThrowMalformedEventException() {
        // given
        Message<?> withoutId = MessageBuilder.withPayload("{}").build();

        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(withoutId))
                .isInstanceOf(MalformedEventException.class)
                .hasMessageStartingWith("Message without a usable event id");
        assertThat(inbox.consumedIds()).isEmpty();
    }

    @Test
    @DisplayName("UT onOrderEvent() when the event id header is not a UUID should refuse it as malformed")
    void onOrderEvent_whenEventIdIsNotUuid_shouldThrowMalformedEventException() {
        // given
        Message<?> badId = MessageBuilder.withPayload("{}").setHeader("oncebox_event_id", "not-a-uuid").build();

        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(badId))
                .isInstanceOf(MalformedEventException.class);
    }

    @Test
    @DisplayName("UT onOrderEvent() when the event is new should log inbox.event.consumed under its ids")
    void onOrderEvent_whenEventIsNew_shouldLogConsumedWithIds() {
        // given
        Message<?> message = MessageBuilder.fromMessage(message(created()))
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "orders.events")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 3)
                .setHeader(KafkaHeaders.OFFSET, 41L)
                .build();

        // when
        List<JsonNode> lines = logsOf(() -> tested.onOrderEvent(message));

        // then
        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line.get("event.action").asText()).isEqualTo("inbox.event.consumed");
            assertThat(line.get("eventId").asText()).isEqualTo(EVENT_ID.toString());
            assertThat(line.get("orderId").asText()).isEqualTo("7");
            assertThat(line.get("customerId").asText()).isEqualTo("42");
            assertThat(line.get("kafka.partition").asText()).isEqualTo("3");
            assertThat(line.get("kafka.offset").asText()).isEqualTo("41");
        });
    }

    @Test
    @DisplayName("UT onOrderEvent() when the event was already consumed should log inbox.event.skipped and not accrue")
    void onOrderEvent_whenEventIsRepeated_shouldLogSkippedAndNotAccrue() {
        // given
        tested.onOrderEvent(message(created()));

        // when
        List<JsonNode> lines = logsOf(() -> tested.onOrderEvent(message(created())));

        // then
        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line.get("event.action").asText()).isEqualTo("inbox.event.skipped");
            assertThat(line.get("eventId").asText()).isEqualTo(EVENT_ID.toString());
        });
        verify(points, times(1)).accrue(created());
    }

    @Test
    @DisplayName("UT onOrderEvent() when handling fails should log inbox.event.failed and rethrow to the error handler")
    void onOrderEvent_whenHandlingFails_shouldLogFailedAndRethrow() {
        // given
        doThrow(new IllegalStateException("database is gone")).when(points).accrue(created());
        List<JsonNode> lines;

        // when
        try (CapturedLogs logs = CapturedLogs.of(OrderEventsListener.class)) {
            assertThatThrownBy(() -> tested.onOrderEvent(message(created())))
                    .isInstanceOf(IllegalStateException.class);
            lines = logs.lines();
        }

        // then
        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line.get("event.action").asText()).isEqualTo("inbox.event.failed");
            assertThat(line.get("level").asText()).isEqualTo("WARN");
            assertThat(line.get("message").asText()).contains("database is gone");
        });
    }

    @Test
    @DisplayName("UT onOrderEvent() when the record is handled should leave no event fields on the listener thread")
    void onOrderEvent_whenRecordIsHandled_shouldClearMdc() {
        // when
        tested.onOrderEvent(message(created()));

        // then
        assertThat(MDC.get("eventId")).isNull();
        assertThat(MDC.get("orderId")).isNull();
    }

    private static OrderCreatedEvent created() {
        return new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
    }

    private static List<JsonNode> logsOf(Runnable action) {
        try (CapturedLogs logs = CapturedLogs.of(OrderEventsListener.class)) {
            action.run();
            return logs.lines();
        }
    }

    private static Message<?> message(Object payload) {
        return MessageBuilder.withPayload(payload)
                .setHeader("oncebox_event_id", EVENT_ID.toString())
                .build();
    }
}

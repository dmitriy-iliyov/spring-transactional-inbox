package io.github.inbox.notification.events;

import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import io.github.inbox.notification.mail.OrderConfirmationMailer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class OrderEventsListenerUnitTest {

    private static final UUID EVENT_ID_1 = UUID.fromString("0195f0a0-0000-7000-8000-000000000001");
    private static final UUID EVENT_ID_2 = UUID.fromString("0195f0a0-0000-7000-8000-000000000002");
    private static final UUID EVENT_ID_3 = UUID.fromString("0195f0a0-0000-7000-8000-000000000003");
    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    @Mock
    private OrderConfirmationMailer mailer;

    private PassThroughIdempotentConsumer inbox;
    private OrderEventsListener tested;

    @BeforeEach
    void setUp() {
        inbox = new PassThroughIdempotentConsumer();
        tested = new OrderEventsListener(inbox, mailer);
    }

    @Test
    @DisplayName("UT onOrderEvent() when a mixed batch arrives should send confirmations for created and skip cancelled")
    void onOrderEvent_whenMixedBatchArrives_shouldSendConfirmationsForCreatedAndSkipCancelled() {
        // given
        OrderCreatedEvent created1 = new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
        OrderCancelledEvent cancelled = new OrderCancelledEvent(8L, 43L, null, NOW);
        OrderCreatedEvent created2 = new OrderCreatedEvent(9L, 44L, "john.doe@example.com",
                List.of(new OrderLine("sku-2", 2, BigDecimal.TWO)), BigDecimal.TWO, "EUR", NOW);

        List<Message<?>> batch = List.of(
                message(EVENT_ID_1, created1),
                message(EVENT_ID_2, cancelled),
                message(EVENT_ID_3, created2)
        );

        // when
        tested.onOrderEvent(batch);

        // then
        verify(mailer).sendConfirmation(EVENT_ID_1, created1);
        verify(mailer).sendConfirmation(EVENT_ID_3, created2);
        verifyNoMoreInteractions(mailer);
        assertThat(inbox.consumedIds()).containsExactly(EVENT_ID_1, EVENT_ID_2, EVENT_ID_3);
    }

    @Test
    @DisplayName("UT onOrderEvent() when a record has an invalid event id header at index 2 should fail the batch at index 2")
    void onOrderEvent_whenEventIdIsNotUuidAtIndex2_shouldFailBatchAtIndex2() {
        // given
        OrderCreatedEvent created1 = new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
        OrderCancelledEvent cancelled = new OrderCancelledEvent(8L, 43L, null, NOW);
        Message<?> badId = MessageBuilder.withPayload("{}").setHeader("oncebox_event_id", "not-a-uuid").build();

        List<Message<?>> batch = List.of(
                message(EVENT_ID_1, created1),
                message(EVENT_ID_2, cancelled),
                badId
        );

        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(batch))
                .isInstanceOf(BatchListenerFailedException.class)
                .hasCauseInstanceOf(MalformedEventException.class)
                .satisfies(ex -> assertThat(((BatchListenerFailedException) ex).getIndex()).isEqualTo(2));

        verify(mailer).sendConfirmation(EVENT_ID_1, created1);
        assertThat(inbox.consumedIds()).containsExactly(EVENT_ID_1, EVENT_ID_2);
    }

    @Test
    @DisplayName("UT onOrderEvent() when a record is missing an event id header at index 2 should fail the batch at index 2")
    void onOrderEvent_whenEventIdHeaderIsMissingAtIndex2_shouldFailBatchAtIndex2() {
        // given
        OrderCreatedEvent created1 = new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
        OrderCancelledEvent cancelled = new OrderCancelledEvent(8L, 43L, null, NOW);
        Message<?> missingId = MessageBuilder.withPayload("{}").build();

        List<Message<?>> batch = List.of(
                message(EVENT_ID_1, created1),
                message(EVENT_ID_2, cancelled),
                missingId
        );

        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(batch))
                .isInstanceOf(BatchListenerFailedException.class)
                .hasCauseInstanceOf(MalformedEventException.class)
                .satisfies(ex -> assertThat(((BatchListenerFailedException) ex).getIndex()).isEqualTo(2));

        verify(mailer).sendConfirmation(EVENT_ID_1, created1);
        assertThat(inbox.consumedIds()).containsExactly(EVENT_ID_1, EVENT_ID_2);
    }

    @Test
    @DisplayName("UT onOrderEvent() when a record has an unknown payload should fail the batch with unsupported event cause")
    void onOrderEvent_whenPayloadIsUnknown_shouldFailBatchWithUnsupportedEventCause() {
        // given
        Message<?> unknown = message(EVENT_ID_1, "something else");

        // when / then
        assertThatThrownBy(() -> tested.onOrderEvent(List.of(unknown)))
                .isInstanceOf(BatchListenerFailedException.class)
                .hasCauseInstanceOf(UnsupportedEventException.class)
                .satisfies(ex -> assertThat(((BatchListenerFailedException) ex).getIndex()).isZero());

        verifyNoInteractions(mailer);
        assertThat(inbox.consumedIds()).isEmpty();
    }

    @Test
    @DisplayName("UT onOrderEvents() when batch arrives should delegate to onOrderEvent")
    void onOrderEvents_whenBatchArrives_shouldDelegateToOnOrderEvent() {
        // given
        OrderCreatedEvent created = new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, BigDecimal.TEN)), BigDecimal.TEN, "EUR", NOW);
        List<Message<?>> batch = List.of(message(EVENT_ID_1, created));

        // when
        tested.onOrderEvents(batch);

        // then
        verify(mailer).sendConfirmation(EVENT_ID_1, created);
        assertThat(inbox.consumedIds()).containsExactly(EVENT_ID_1);
    }

    private static Message<?> message(UUID eventId, Object payload) {
        return MessageBuilder.withPayload(payload)
                .setHeader("oncebox_event_id", eventId.toString())
                .build();
    }
}

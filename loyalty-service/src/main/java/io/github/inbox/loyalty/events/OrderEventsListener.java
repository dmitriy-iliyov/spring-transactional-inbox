package io.github.inbox.loyalty.events;

import io.github.dmitriyiliyov.oncebox.core.OnceboxHeadersUtils;
import io.github.dmitriyiliyov.oncebox.core.inbox.IdempotentConsumer;
import io.github.dmitriyiliyov.oncebox.starter.inbox.TransactionalConsumer;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.Topics;
import io.github.inbox.loyalty.logging.ConsumedEventLogContext;
import io.github.inbox.loyalty.points.PointsService;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ACTION;
import static io.github.inbox.contract.ObservabilityVocabulary.INBOX_EVENT_CONSUMED;
import static io.github.inbox.contract.ObservabilityVocabulary.INBOX_EVENT_FAILED;
import static io.github.inbox.contract.ObservabilityVocabulary.INBOX_EVENT_SKIPPED;

/**
 * Turns order events into points, each event at most once in effect.
 * <p>
 * The transactional inbox records the event id in the transaction that changes the points: a failure rolls both back
 * and the redelivered event is handled again, a repeated event that was handled is skipped.
 * <p>
 * Every record is logged once with its outcome: {@code inbox.event.consumed} after the transaction committed,
 * {@code inbox.event.skipped} when the inbox already knew the event, {@code inbox.event.failed} when handling threw
 * and the record goes back to the error handler.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final IdempotentConsumer inbox;
    private final PointsService points;

    public OrderEventsListener(@TransactionalConsumer IdempotentConsumer inbox, PointsService points) {
        this.inbox = inbox;
        this.points = points;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, containerFactory = "inboxKafkaListenerContainerFactory")
    public void onOrderEvent(Message<?> message) {
        try (ConsumedEventLogContext ignored = ConsumedEventLogContext.open(message)) {
            AtomicBoolean handled = new AtomicBoolean();
            try {
                inbox.consume(message, OrderEventsListener::eventId, received -> {
                    handle(received);
                    handled.set(true);
                });
            } catch (RuntimeException e) {
                log.warn(Markers.append(EVENT_ACTION, INBOX_EVENT_FAILED), "Event handling failed: {}", e.toString());
                throw e;
            }
            if (handled.get()) {
                log.info(Markers.append(EVENT_ACTION, INBOX_EVENT_CONSUMED), "Event consumed");
            } else {
                log.info(Markers.append(EVENT_ACTION, INBOX_EVENT_SKIPPED), "Event already consumed, skipped");
            }
        }
    }

    private static UUID eventId(Message<?> message) {
        try {
            return OnceboxHeadersUtils.extractId(message);
        } catch (IllegalArgumentException e) {
            throw new MalformedEventException("Message without a usable event id: " + e.getMessage(), e);
        }
    }

    private void handle(Message<?> message) {
        switch (message.getPayload()) {
            case OrderCreatedEvent created -> points.accrue(created);
            case OrderCancelledEvent cancelled -> points.revert(cancelled);
            default -> throw new UnsupportedEventException(message.getPayload());
        }
    }
}

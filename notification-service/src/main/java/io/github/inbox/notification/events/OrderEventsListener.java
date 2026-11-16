package io.github.inbox.notification.events;

import io.github.dmitriyiliyov.oncebox.core.OnceboxHeadersUtils;
import io.github.dmitriyiliyov.oncebox.core.inbox.IdempotentConsumer;
import io.github.dmitriyiliyov.oncebox.starter.inbox.NonTransactionalConsumer;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.Topics;
import io.github.inbox.notification.mail.OrderConfirmationMailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Sends an order confirmation for every placed order, at most once per event.
 * <p>
 * An e-mail cannot be taken back, so the inbox records the event before the e-mail is sent: a repeated event is
 * skipped, and so is a redelivery of one whose sending failed - that confirmation is not sent, rather than possibly
 * sent twice. The library logs such a failure with the ids it will not process again.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final IdempotentConsumer inbox;
    private final OrderConfirmationMailer mailer;

    public OrderEventsListener(@NonTransactionalConsumer IdempotentConsumer inbox, OrderConfirmationMailer mailer) {
        this.inbox = inbox;
        this.mailer = mailer;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, containerFactory = "inboxKafkaListenerContainerFactory")
    public void onOrderEvent(List<Message<?>> messages) {
        for (int i = 0; i < messages.size(); i++) {
            Message<?> message = messages.get(i);
            try {
                validate(message);
            } catch (MalformedEventException | UnsupportedEventException e) {
                if (i > 0) {
                    inbox.consume(messages.subList(0, i), OrderEventsListener::eventId, this::handleAll);
                }
                throw new BatchListenerFailedException(e.getMessage(), e, i);
            }
        }
        inbox.consume(messages, OrderEventsListener::eventId, this::handleAll);
    }

    public void onOrderEvents(List<Message<?>> messages) {
        onOrderEvent(messages);
    }

    private static void validate(Message<?> message) {
        eventId(message);
        Object payload = message.getPayload();
        if (!(payload instanceof OrderCreatedEvent || payload instanceof OrderCancelledEvent)) {
            throw new UnsupportedEventException(payload);
        }
    }

    private static UUID eventId(Message<?> message) {
        try {
            return OnceboxHeadersUtils.extractId(message);
        } catch (IllegalArgumentException e) {
            throw new MalformedEventException("Message without a usable event id: " + e.getMessage(), e);
        }
    }

    private void handleAll(List<Message<?>> messages) {
        for (Message<?> message : messages) {
            handle(message);
        }
    }

    private void handle(Message<?> message) {
        switch (message.getPayload()) {
            case OrderCreatedEvent created -> mailer.sendConfirmation(eventId(message), created);
            case OrderCancelledEvent cancelled -> log.debug("Order {} cancelled, no e-mail to send", cancelled.orderId());
            default -> throw new UnsupportedEventException(message.getPayload());
        }
    }
}

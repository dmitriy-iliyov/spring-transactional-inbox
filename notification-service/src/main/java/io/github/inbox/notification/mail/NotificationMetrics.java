package io.github.inbox.notification.mail;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Counters of confirmations the mail server accepted and refused. A refused confirmation is not sent again: its
 * event is already recorded in the inbox, so the failed counter is the number of customers left without one.
 */
@Component
public class NotificationMetrics {

    static final String NOTIFICATIONS_SENT = "notifications.sent";
    static final String NOTIFICATIONS_FAILED = "notifications.failed";

    private final Counter sent;
    private final Counter failed;

    public NotificationMetrics(MeterRegistry registry) {
        this.sent = Counter.builder(NOTIFICATIONS_SENT)
                .description("Order confirmations accepted by the mail server")
                .register(registry);
        this.failed = Counter.builder(NOTIFICATIONS_FAILED)
                .description("Order confirmations the mail server refused or did not answer")
                .register(registry);
    }

    void sent() {
        sent.increment();
    }

    void failed() {
        failed.increment();
    }
}

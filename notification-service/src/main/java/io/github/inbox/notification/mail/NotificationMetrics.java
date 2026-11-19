package io.github.inbox.notification.mail;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Counters of confirmations the mail server accepted and refused, and timers of the SMTP send. A refused
 * confirmation is not sent again: its event is already recorded in the inbox, so the failed counter is the number
 * of customers left without one.
 */
@Component
public class NotificationMetrics {

    static final String NOTIFICATIONS_SENT = "notifications.sent";
    static final String NOTIFICATIONS_FAILED = "notifications.failed";

    static final String NOTIFICATIONS_SEND_DURATION = "notifications.send.duration";
    static final String OUTCOME = "outcome";

    private final Counter sent;
    private final Counter failed;

    private final MeterRegistry registry;
    private final Timer sendSentTimer;
    private final Timer sendFailedTimer;

    public NotificationMetrics(MeterRegistry registry) {
        this.sent = Counter.builder(NOTIFICATIONS_SENT)
                .description("Order confirmations accepted by the mail server")
                .register(registry);
        this.failed = Counter.builder(NOTIFICATIONS_FAILED)
                .description("Order confirmations the mail server refused or did not answer")
                .register(registry);

        this.registry = registry;
        this.sendSentTimer = sendTimer(registry, "sent");
        this.sendFailedTimer = sendTimer(registry, "failed");
    }

    void sent() {
        sent.increment();
    }

    void failed() {
        failed.increment();
    }

    Timer.Sample startSend() {
        return Timer.start(registry);
    }

    void sendSucceeded(Timer.Sample sample) {
        sample.stop(sendSentTimer);
    }

    void sendFailed(Timer.Sample sample) {
        sample.stop(sendFailedTimer);
    }

    private static Timer sendTimer(MeterRegistry registry, String outcome) {
        return Timer.builder(NOTIFICATIONS_SEND_DURATION)
                .description("Time the mail server takes to accept or refuse a confirmation")
                .tag(OUTCOME, outcome)
                .publishPercentileHistogram()
                .register(registry);
    }
}

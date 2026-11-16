package io.github.inbox.order.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Counters of orders placed and cancelled. An order is counted once its transaction commits, so a rolled back
 * order - one whose outbox row never existed - is not counted. Outside a transaction it is counted at once.
 */
@Component
public class OrderMetrics {

    static final String ORDERS_PLACED = "orders.placed";
    static final String ORDERS_CANCELLED = "orders.cancelled";

    private final Counter placed;
    private final Counter cancelled;

    public OrderMetrics(MeterRegistry registry) {
        this.placed = Counter.builder(ORDERS_PLACED)
                .description("Orders stored together with their order-created event")
                .register(registry);
        this.cancelled = Counter.builder(ORDERS_CANCELLED)
                .description("Orders cancelled together with their order-cancelled event")
                .register(registry);
    }

    void orderPlaced() {
        afterCommit(placed::increment);
    }

    void orderCancelled() {
        afterCommit(cancelled::increment);
    }

    private static void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}

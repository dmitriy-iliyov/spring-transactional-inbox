package io.github.inbox.loyalty.points;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Counters of the points accrued and taken back, in points rather than in events. Points are counted once the inbox
 * transaction commits: a transaction rolled back by a version conflict or a failure is redelivered and counted on
 * the attempt that commits, never twice. Outside a transaction they are counted at once.
 */
@Component
public class PointsMetrics {

    static final String POINTS_ACCRUED = "loyalty.points.accrued";
    static final String POINTS_REVERTED = "loyalty.points.reverted";

    private final Counter accrued;
    private final Counter reverted;

    public PointsMetrics(MeterRegistry registry) {
        this.accrued = Counter.builder(POINTS_ACCRUED)
                .description("Loyalty points accrued for placed orders")
                .register(registry);
        this.reverted = Counter.builder(POINTS_REVERTED)
                .description("Loyalty points taken back for cancelled orders")
                .register(registry);
    }

    /**
     * @param points the points of one ledger entry; a reversal carries them negative and is counted by its size
     */
    void recorded(LedgerEntryKind kind, long points) {
        Counter counter = LedgerEntryKind.ACCRUAL == kind ? accrued : reverted;
        double amount = Math.abs(points);
        afterCommit(() -> counter.increment(amount));
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

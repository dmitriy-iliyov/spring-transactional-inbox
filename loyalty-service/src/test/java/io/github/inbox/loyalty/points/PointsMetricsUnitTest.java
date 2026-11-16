package io.github.inbox.loyalty.points;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;

class PointsMetricsUnitTest {

    private SimpleMeterRegistry meters;
    private PointsMetrics tested;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        tested = new PointsMetrics(meters);
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @DisplayName("UT recorded() when an accrual is recorded outside a transaction should count its points at once")
    void recorded_whenAccrualOutsideTransaction_shouldCountPointsAtOnce() {
        // when
        tested.recorded(LedgerEntryKind.ACCRUAL, 21);

        // then
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isEqualTo(21.0);
        assertThat(count(PointsMetrics.POINTS_REVERTED)).isZero();
    }

    @Test
    @DisplayName("UT recorded() when a reversal carries negative points should count them by their size")
    void recorded_whenReversalIsNegative_shouldCountPointsBySize() {
        // when
        tested.recorded(LedgerEntryKind.REVERSAL, -21);

        // then
        assertThat(count(PointsMetrics.POINTS_REVERTED)).isEqualTo(21.0);
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isZero();
    }

    @Test
    @DisplayName("UT recorded() when the transaction commits should count after the commit")
    void recorded_whenTransactionCommits_shouldCountAfterCommit() {
        // given
        TransactionSynchronizationManager.initSynchronization();

        // when
        tested.recorded(LedgerEntryKind.ACCRUAL, 10);

        // then
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isZero();
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isEqualTo(10.0);
    }

    @Test
    @DisplayName("UT recorded() when the transaction rolls back should not count")
    void recorded_whenTransactionRollsBack_shouldNotCount() {
        // given
        TransactionSynchronizationManager.initSynchronization();

        // when
        tested.recorded(LedgerEntryKind.ACCRUAL, 10);
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        // then
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isZero();
    }

    private double count(String meter) {
        return meters.get(meter).counter().count();
    }
}

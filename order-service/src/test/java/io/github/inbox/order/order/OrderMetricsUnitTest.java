package io.github.inbox.order.order;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;

class OrderMetricsUnitTest {

    private SimpleMeterRegistry meters;
    private OrderMetrics tested;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        tested = new OrderMetrics(meters);
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @DisplayName("UT constructor when registry is empty should register both counters at zero")
    void constructor_whenRegistryIsEmpty_shouldRegisterBothCountersAtZero() {
        // then
        assertThat(count(OrderMetrics.ORDERS_PLACED)).isZero();
        assertThat(count(OrderMetrics.ORDERS_CANCELLED)).isZero();
    }

    @Test
    @DisplayName("UT orderPlaced() when no transaction is active should count at once")
    void orderPlaced_whenNoTransactionIsActive_shouldCountAtOnce() {
        // when
        tested.orderPlaced();

        // then
        assertThat(count(OrderMetrics.ORDERS_PLACED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("UT orderPlaced() when the transaction commits should count after the commit")
    void orderPlaced_whenTransactionCommits_shouldCountAfterCommit() {
        // given
        TransactionSynchronizationManager.initSynchronization();

        // when
        tested.orderPlaced();

        // then
        assertThat(count(OrderMetrics.ORDERS_PLACED)).isZero();
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertThat(count(OrderMetrics.ORDERS_PLACED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("UT orderCancelled() when the transaction rolls back should not count")
    void orderCancelled_whenTransactionRollsBack_shouldNotCount() {
        // given
        TransactionSynchronizationManager.initSynchronization();

        // when
        tested.orderCancelled();
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        // then
        assertThat(count(OrderMetrics.ORDERS_CANCELLED)).isZero();
    }

    @Test
    @DisplayName("UT orderCancelled() when no transaction is active should count only cancellations")
    void orderCancelled_whenNoTransactionIsActive_shouldCountOnlyCancellations() {
        // when
        tested.orderCancelled();

        // then
        assertThat(count(OrderMetrics.ORDERS_CANCELLED)).isEqualTo(1.0);
        assertThat(count(OrderMetrics.ORDERS_PLACED)).isZero();
    }

    private double count(String meter) {
        return meters.get(meter).counter().count();
    }
}

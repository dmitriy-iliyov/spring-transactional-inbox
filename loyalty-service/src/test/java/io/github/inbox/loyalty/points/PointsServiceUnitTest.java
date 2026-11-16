package io.github.inbox.loyalty.points;

import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PointsServiceUnitTest {

    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");
    private static final long ORDER_ID = 7L;
    private static final long CUSTOMER_ID = 42L;

    @Mock
    private CustomerPointsRepository balances;

    @Mock
    private LedgerEntryRepository ledger;

    private SimpleMeterRegistry meters;
    private PointsService tested;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        tested = new PointsService(balances, ledger, new PointsPolicy(), new PointsMetrics(meters),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("UT accrue() when customer has no balance yet should open one with the order's points")
    void accrue_whenCustomerHasNoBalance_shouldOpenBalanceWithPoints() {
        // given
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL)).thenReturn(Optional.empty());
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL)).thenReturn(Optional.empty());
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.empty());

        // when
        tested.accrue(created("21.50"));

        // then
        LedgerEntryEntity entry = savedLedgerEntry();
        assertThat(entry.getKind()).isEqualTo(LedgerEntryKind.ACCRUAL);
        assertThat(entry.getPoints()).isEqualTo(21);
        CustomerPointsEntity balance = savedBalance();
        assertThat(balance.getBalance()).isEqualTo(21);
        assertThat(balance.getUpdatedAt()).isEqualTo(NOW);
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isEqualTo(21.0);
    }

    @Test
    @DisplayName("UT accrue() when customer has a balance should add the order's points to it")
    void accrue_whenCustomerHasBalance_shouldAddPoints() {
        // given
        CustomerPointsEntity existing = new CustomerPointsEntity(CUSTOMER_ID, NOW.minusSeconds(60));
        existing.add(100, NOW.minusSeconds(60));
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL)).thenReturn(Optional.empty());
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL)).thenReturn(Optional.empty());
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.of(existing));

        // when
        tested.accrue(created("10.00"));

        // then
        assertThat(savedBalance().getBalance()).isEqualTo(110);
    }

    @Test
    @DisplayName("UT accrue() when the order was already cancelled should accrue nothing")
    void accrue_whenOrderWasCancelledFirst_shouldAccrueNothing() {
        // given
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL)).thenReturn(Optional.empty());
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL))
                .thenReturn(Optional.of(new LedgerEntryEntity(ORDER_ID, CUSTOMER_ID, LedgerEntryKind.REVERSAL, 0, NOW)));

        // when
        tested.accrue(created("10.00"));

        // then
        verify(ledger, never()).save(any());
        verifyNoInteractions(balances);
    }

    @Test
    @DisplayName("UT revert() when the order earned points should take exactly those points back")
    void revert_whenOrderEarnedPoints_shouldTakeThemBack() {
        // given
        CustomerPointsEntity existing = new CustomerPointsEntity(CUSTOMER_ID, NOW.minusSeconds(60));
        existing.add(21, NOW.minusSeconds(60));
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL)).thenReturn(Optional.empty());
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL))
                .thenReturn(Optional.of(new LedgerEntryEntity(ORDER_ID, CUSTOMER_ID, LedgerEntryKind.ACCRUAL, 21, NOW)));
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.of(existing));

        // when
        tested.revert(cancelled());

        // then
        LedgerEntryEntity entry = savedLedgerEntry();
        assertThat(entry.getKind()).isEqualTo(LedgerEntryKind.REVERSAL);
        assertThat(entry.getPoints()).isEqualTo(-21);
        assertThat(savedBalance().getBalance()).isZero();
        assertThat(count(PointsMetrics.POINTS_REVERTED)).isEqualTo(21.0);
    }

    @Test
    @DisplayName("UT revert() when the order earned nothing yet should record a reversal of zero points")
    void revert_whenOrderEarnedNothingYet_shouldRecordZeroReversal() {
        // given
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL)).thenReturn(Optional.empty());
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL)).thenReturn(Optional.empty());
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.empty());

        // when
        tested.revert(cancelled());

        // then
        assertThat(savedLedgerEntry().getPoints()).isZero();
        assertThat(savedBalance().getBalance()).isZero();
    }

    @Test
    @DisplayName("UT accrue() when the order already earned its points should change nothing")
    void accrue_whenAccrualAlreadyRecorded_shouldChangeNothing() {
        // given
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.ACCRUAL))
                .thenReturn(Optional.of(new LedgerEntryEntity(ORDER_ID, CUSTOMER_ID, LedgerEntryKind.ACCRUAL, 21, NOW)));

        // when
        tested.accrue(created("21.00"));

        // then
        verify(ledger, never()).save(any());
        verifyNoInteractions(balances);
        assertThat(count(PointsMetrics.POINTS_ACCRUED)).isZero();
    }

    @Test
    @DisplayName("UT revert() when the order's points were already taken back should change nothing")
    void revert_whenReversalAlreadyRecorded_shouldChangeNothing() {
        // given
        when(ledger.findByOrderIdAndKind(ORDER_ID, LedgerEntryKind.REVERSAL))
                .thenReturn(Optional.of(new LedgerEntryEntity(ORDER_ID, CUSTOMER_ID, LedgerEntryKind.REVERSAL, -21, NOW)));

        // when
        tested.revert(cancelled());

        // then
        verify(ledger, never()).save(any());
        verifyNoInteractions(balances);
    }

    @Test
    @DisplayName("UT balance() when the customer has a balance should return it")
    void balance_whenCustomerHasBalance_shouldReturnIt() {
        // given
        CustomerPointsEntity existing = new CustomerPointsEntity(CUSTOMER_ID, NOW);
        existing.add(120, NOW);
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.of(existing));

        // when / then
        assertThat(tested.balance(CUSTOMER_ID).balance()).isEqualTo(120);
    }

    @Test
    @DisplayName("UT balance() when no event of the customer was handled should throw CustomerPointsNotFoundException")
    void balance_whenCustomerIsUnknown_shouldThrowCustomerPointsNotFoundException() {
        // given
        when(balances.findById(CUSTOMER_ID)).thenReturn(Optional.empty());

        // when / then
        assertThatThrownBy(() -> tested.balance(CUSTOMER_ID))
                .isInstanceOf(CustomerPointsNotFoundException.class)
                .hasMessage("Customer 42 has no points yet");
    }

    private double count(String meter) {
        return meters.get(meter).counter().count();
    }

    private LedgerEntryEntity savedLedgerEntry() {
        ArgumentCaptor<LedgerEntryEntity> captor = ArgumentCaptor.forClass(LedgerEntryEntity.class);
        verify(ledger).save(captor.capture());
        return captor.getValue();
    }

    private CustomerPointsEntity savedBalance() {
        ArgumentCaptor<CustomerPointsEntity> captor = ArgumentCaptor.forClass(CustomerPointsEntity.class);
        verify(balances).save(captor.capture());
        return captor.getValue();
    }

    private static OrderCreatedEvent created(String total) {
        return new OrderCreatedEvent(ORDER_ID, CUSTOMER_ID, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, new BigDecimal(total))), new BigDecimal(total), "EUR", NOW);
    }

    private static OrderCancelledEvent cancelled() {
        return new OrderCancelledEvent(ORDER_ID, CUSTOMER_ID, "Ordered by mistake", NOW);
    }
}

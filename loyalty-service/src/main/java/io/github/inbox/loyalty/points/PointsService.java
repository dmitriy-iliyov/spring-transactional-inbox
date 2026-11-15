package io.github.inbox.loyalty.points;

import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.loyalty.api.PointsBalanceResponse;
import io.github.inbox.contract.OrderCreatedEvent;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ACTION;
import static io.github.inbox.contract.ObservabilityVocabulary.POINTS_ACCRUED;
import static io.github.inbox.contract.ObservabilityVocabulary.POINTS_REVERTED;

/**
 * Keeps the ledger and the balances in step with order events.
 * <p>
 * Events of one order arrive in no guaranteed order, so a cancellation that comes first is recorded as a reversal of
 * zero points, and an accrual that comes after it is not made: the balance ends the same whichever event was first.
 * A repeated event is normally stopped by the inbox in front of this class. One replayed after the inbox has
 * forgotten its id (past {@code oncebox.inbox.clean-up.ttl}) finds its ledger entry already there and changes nothing.
 */
@Service
public class PointsService {

    private static final Logger log = LoggerFactory.getLogger(PointsService.class);

    private final CustomerPointsRepository balances;
    private final LedgerEntryRepository ledger;
    private final PointsPolicy policy;
    private final Clock clock;

    public PointsService(CustomerPointsRepository balances,
                         LedgerEntryRepository ledger,
                         PointsPolicy policy,
                         Clock clock) {
        this.balances = balances;
        this.ledger = ledger;
        this.policy = policy;
        this.clock = clock;
    }

    @Transactional
    public void accrue(OrderCreatedEvent event) {
        if (isApplied(event.orderId(), LedgerEntryKind.ACCRUAL)) {
            return;
        }
        if (ledger.findByOrderIdAndKind(event.orderId(), LedgerEntryKind.REVERSAL).isPresent()) {
            log.info("Order {} was cancelled before its points were accrued, nothing to accrue", event.orderId());
            return;
        }
        long points = policy.pointsFor(event.totalAmount());
        record(event.orderId(), event.customerId(), LedgerEntryKind.ACCRUAL, points);
    }

    @Transactional
    public void revert(OrderCancelledEvent event) {
        if (isApplied(event.orderId(), LedgerEntryKind.REVERSAL)) {
            return;
        }
        long points = ledger.findByOrderIdAndKind(event.orderId(), LedgerEntryKind.ACCRUAL)
                .map(accrual -> -accrual.getPoints())
                .orElse(0L);
        record(event.orderId(), event.customerId(), LedgerEntryKind.REVERSAL, points);
    }

    /**
     * @throws CustomerPointsNotFoundException if no event of the customer has been handled yet
     */
    @Transactional(readOnly = true)
    public PointsBalanceResponse balance(Long customerId) {
        CustomerPointsEntity balance = balances.findById(customerId)
                .orElseThrow(() -> new CustomerPointsNotFoundException(customerId));
        return new PointsBalanceResponse(balance.getCustomerId(), balance.getBalance(), balance.getUpdatedAt());
    }

    private boolean isApplied(Long orderId, LedgerEntryKind kind) {
        boolean applied = ledger.findByOrderIdAndKind(orderId, kind).isPresent();
        if (applied) {
            log.info("Order {} already has its {} entry, the event is a replay and changes nothing", orderId, kind);
        }
        return applied;
    }

    private void record(Long orderId, Long customerId, LedgerEntryKind kind, long points) {
        Instant now = clock.instant();
        ledger.save(new LedgerEntryEntity(orderId, customerId, kind, points, now));
        CustomerPointsEntity balance = balances.findById(customerId)
                .orElseGet(() -> new CustomerPointsEntity(customerId, now));
        balance.add(points, now);
        balances.save(balance);
        String action = LedgerEntryKind.ACCRUAL == kind ? POINTS_ACCRUED : POINTS_REVERTED;
        log.info(Markers.append(EVENT_ACTION, action), "Order {} {}: {} points for customer {}",
                orderId, kind, points, customerId);
    }
}

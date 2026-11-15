package io.github.inbox.order.order;

import io.github.inbox.contract.EventTypes;
import io.github.inbox.order.api.CancelOrderRequest;
import io.github.inbox.order.api.CreateOrderRequest;
import io.github.inbox.order.api.OrderResponse;
import io.github.inbox.order.events.EventPublisher;
import io.github.inbox.order.persistence.OrderEntity;
import io.github.inbox.order.persistence.OrderRepository;
import io.github.inbox.order.persistence.OrderStatus;
import net.logstash.logback.marker.LogstashMarker;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

import static io.github.inbox.contract.ObservabilityVocabulary.CUSTOMER_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ACTION;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ID;
import static io.github.inbox.contract.ObservabilityVocabulary.ORDER_CANCELLED;
import static io.github.inbox.contract.ObservabilityVocabulary.ORDER_CREATED;
import static io.github.inbox.contract.ObservabilityVocabulary.ORDER_ID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository repository;
    private final OrderMapper mapper;
    private final EventPublisher events;
    private final Clock clock;

    public OrderService(OrderRepository repository, OrderMapper mapper, EventPublisher events, Clock clock) {
        this.repository = repository;
        this.mapper = mapper;
        this.events = events;
        this.clock = clock;
    }

    /**
     * Stores a new order and publishes {@link EventTypes#ORDER_CREATED} in the same transaction.
     */
    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        OrderEntity order = new OrderEntity(
                request.customerId(),
                request.customerEmail(),
                request.currency(),
                mapper.toLineEntities(request.lines()),
                clock.instant()
        );
        OrderEntity stored = repository.save(order);
        UUID eventId = events.publish(EventTypes.ORDER_CREATED, mapper.toCreatedEvent(stored));
        log.info(fields(ORDER_CREATED, stored, eventId), "Order {} created for customer {}, event {}",
                stored.getId(), stored.getCustomerId(), eventId);
        return mapper.toResponse(stored);
    }

    /**
     * @throws OrderNotFoundException if there is no order with this id
     */
    @Transactional(readOnly = true)
    public OrderResponse get(Long orderId) {
        return mapper.toResponse(find(orderId));
    }

    /**
     * Cancels a created order and publishes {@link EventTypes#ORDER_CANCELLED} in the same transaction. Cancelling
     * an already cancelled order is refused rather than ignored, so a client retrying with another reason learns that
     * the first cancellation stands.
     * <p>
     * The cancellation is flushed before the event is published, so of two cancellations racing on one order the
     * second meets the first one's version, publishes nothing and is refused like any later cancellation.
     *
     * @throws OrderNotFoundException         if there is no order with this id
     * @throws OrderAlreadyCancelledException if the order is already cancelled, or a concurrent request cancelled it
     */
    @Transactional
    public OrderResponse cancel(Long orderId, CancelOrderRequest request) {
        OrderEntity order = find(orderId);
        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new OrderAlreadyCancelledException(orderId);
        }
        order.cancel(request.reason(), clock.instant());
        try {
            repository.saveAndFlush(order);
        } catch (OptimisticLockingFailureException e) {
            // cancelling is the only change an order takes, so whoever changed it first cancelled it
            throw new OrderAlreadyCancelledException(orderId);
        }
        UUID eventId = events.publish(EventTypes.ORDER_CANCELLED, mapper.toCancelledEvent(order));
        log.info(fields(ORDER_CANCELLED, order, eventId), "Order {} of customer {} cancelled, event {}",
                order.getId(), order.getCustomerId(), eventId);
        return mapper.toResponse(order);
    }

    // ids go out as strings, as they do in the MDC, so a field has one type in every log line
    private static LogstashMarker fields(String action, OrderEntity order, UUID eventId) {
        return Markers.appendEntries(Map.of(
                EVENT_ACTION, action,
                ORDER_ID, String.valueOf(order.getId()),
                CUSTOMER_ID, String.valueOf(order.getCustomerId()),
                EVENT_ID, String.valueOf(eventId)
        ));
    }

    private OrderEntity find(Long orderId) {
        return repository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}

package io.github.inbox.order.order;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.inbox.contract.EventTypes;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.order.api.CancelOrderRequest;
import io.github.inbox.order.api.CreateOrderRequest;
import io.github.inbox.order.api.OrderLineRequest;
import io.github.inbox.order.api.OrderResponse;
import io.github.inbox.order.events.EventPublisher;
import io.github.inbox.order.persistence.OrderEntity;
import io.github.inbox.order.persistence.OrderLineEntity;
import io.github.inbox.order.persistence.OrderRepository;
import io.github.inbox.order.persistence.OrderStatus;
import io.github.inbox.order.support.CapturedLogs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceUnitTest {

    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");
    private static final UUID EVENT_ID = UUID.fromString("0195f0a0-0000-7000-8000-000000000001");

    @Mock
    private OrderRepository repository;

    @Mock
    private EventPublisher events;

    private OrderService tested;

    @BeforeEach
    void setUp() {
        tested = new OrderService(repository, new OrderMapper(), events, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("UT create() when request is valid should store a created order stamped with the current time")
    void create_whenRequestIsValid_shouldStoreCreatedOrder() {
        // given
        when(repository.save(any(OrderEntity.class))).thenAnswer(invocation -> withId(invocation.getArgument(0), 7L));
        CreateOrderRequest request = new CreateOrderRequest(42L, "customer@example.com", "EUR",
                List.of(new OrderLineRequest("sku-1", 2, new BigDecimal("10.50"))));

        // when
        OrderResponse response = tested.create(request);

        // then
        ArgumentCaptor<OrderEntity> stored = ArgumentCaptor.forClass(OrderEntity.class);
        verify(repository).save(stored.capture());
        assertThat(stored.getValue().getCreatedAt()).isEqualTo(NOW);
        assertThat(response.status()).isEqualTo(OrderStatus.CREATED);
        assertThat(response.totalAmount()).isEqualByComparingTo("21.00");
        assertThat(response.lines()).singleElement().satisfies(line -> assertThat(line.productId()).isEqualTo("sku-1"));
    }

    @Test
    @DisplayName("UT create() when order is stored should publish order-created with its lines and total")
    void create_whenOrderIsStored_shouldPublishOrderCreated() {
        // given
        when(repository.save(any(OrderEntity.class))).thenAnswer(invocation -> withId(invocation.getArgument(0), 7L));
        CreateOrderRequest request = new CreateOrderRequest(42L, "customer@example.com", "EUR",
                List.of(new OrderLineRequest("sku-1", 2, new BigDecimal("10.50"))));

        // when
        tested.create(request);

        // then
        ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
        verify(events).publish(eq(EventTypes.ORDER_CREATED), event.capture());
        assertThat(event.getValue()).isInstanceOfSatisfying(OrderCreatedEvent.class, created -> {
            assertThat(created.customerEmail()).isEqualTo("customer@example.com");
            assertThat(created.totalAmount()).isEqualByComparingTo("21.00");
            assertThat(created.createdAt()).isEqualTo(NOW);
            assertThat(created.lines()).singleElement().satisfies(line -> assertThat(line.quantity()).isEqualTo(2));
        });
    }

    @Test
    @DisplayName("UT create() when the order is published should log order.created with order, customer and event id")
    void create_whenOrderIsPublished_shouldLogOrderCreatedWithIds() {
        // given
        when(repository.save(any(OrderEntity.class))).thenAnswer(invocation -> withId(invocation.getArgument(0), 7L));
        when(events.publish(eq(EventTypes.ORDER_CREATED), any())).thenReturn(EVENT_ID);
        CreateOrderRequest request = new CreateOrderRequest(42L, "customer@example.com", "EUR",
                List.of(new OrderLineRequest("sku-1", 2, new BigDecimal("10.50"))));

        // when
        List<JsonNode> lines;
        try (CapturedLogs logs = CapturedLogs.of(OrderService.class)) {
            tested.create(request);
            lines = logs.lines();
        }

        // then
        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line.get("event.action").asText()).isEqualTo("order.created");
            assertThat(line.get("orderId").asText()).isEqualTo("7");
            assertThat(line.get("customerId").asText()).isEqualTo("42");
            assertThat(line.get("eventId").asText()).isEqualTo(EVENT_ID.toString());
        });
    }

    @Test
    @DisplayName("UT get() when order does not exist should throw OrderNotFoundException")
    void get_whenOrderDoesNotExist_shouldThrowOrderNotFoundException() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.empty());

        // when / then
        assertThatThrownBy(() -> tested.get(7L))
                .isInstanceOf(OrderNotFoundException.class)
                .hasMessage("Order 7 not found");
    }

    @Test
    @DisplayName("UT get() when order exists should return it")
    void get_whenOrderExists_shouldReturnIt() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.of(newOrder()));

        // when
        OrderResponse response = tested.get(7L);

        // then
        assertThat(response.customerId()).isEqualTo(42L);
        assertThat(response.status()).isEqualTo(OrderStatus.CREATED);
    }

    @Test
    @DisplayName("UT cancel() when order is created should cancel it with the reason and the current time")
    void cancel_whenOrderIsCreated_shouldCancelIt() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.of(newOrder()));

        // when
        OrderResponse response = tested.cancel(7L, new CancelOrderRequest("changed my mind"));

        // then
        assertThat(response.status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(response.cancelReason()).isEqualTo("changed my mind");
        assertThat(response.cancelledAt()).isEqualTo(NOW);
        ArgumentCaptor<Object> event = ArgumentCaptor.forClass(Object.class);
        verify(events).publish(eq(EventTypes.ORDER_CANCELLED), event.capture());
        assertThat(event.getValue()).isInstanceOfSatisfying(OrderCancelledEvent.class, cancelled -> {
            assertThat(cancelled.reason()).isEqualTo("changed my mind");
            assertThat(cancelled.cancelledAt()).isEqualTo(NOW);
        });
    }

    @Test
    @DisplayName("UT cancel() when the cancellation is published should log order.cancelled with its event id")
    void cancel_whenCancellationIsPublished_shouldLogOrderCancelledWithIds() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.of(newOrder()));
        when(events.publish(eq(EventTypes.ORDER_CANCELLED), any())).thenReturn(EVENT_ID);

        // when
        List<JsonNode> lines;
        try (CapturedLogs logs = CapturedLogs.of(OrderService.class)) {
            tested.cancel(7L, new CancelOrderRequest("changed my mind"));
            lines = logs.lines();
        }

        // then
        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line.get("event.action").asText()).isEqualTo("order.cancelled");
            assertThat(line.get("orderId").asText()).isEqualTo("7");
            assertThat(line.get("eventId").asText()).isEqualTo(EVENT_ID.toString());
        });
    }

    @Test
    @DisplayName("UT cancel() when order is already cancelled should throw OrderAlreadyCancelledException")
    void cancel_whenOrderIsAlreadyCancelled_shouldThrowOrderAlreadyCancelledException() {
        // given
        OrderEntity order = newOrder();
        order.cancel("first", NOW.minusSeconds(60));
        when(repository.findById(7L)).thenReturn(Optional.of(order));

        // when / then
        assertThatThrownBy(() -> tested.cancel(7L, new CancelOrderRequest("second")))
                .isInstanceOf(OrderAlreadyCancelledException.class)
                .hasMessage("Order 7 is already cancelled");
        assertThat(order.getCancelReason()).isEqualTo("first");
        verifyNoInteractions(events);
    }

    @Test
    @DisplayName("UT cancel() when a concurrent request cancelled the order first should refuse and publish nothing")
    void cancel_whenConcurrentRequestCancelledFirst_shouldThrowOrderAlreadyCancelledException() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.of(newOrder()));
        when(repository.saveAndFlush(any(OrderEntity.class)))
                .thenThrow(new ObjectOptimisticLockingFailureException(OrderEntity.class, 7L));

        // when / then
        assertThatThrownBy(() -> tested.cancel(7L, new CancelOrderRequest("second")))
                .isInstanceOf(OrderAlreadyCancelledException.class)
                .hasMessage("Order 7 is already cancelled");
        verifyNoInteractions(events);
    }

    @Test
    @DisplayName("UT cancel() when order does not exist should throw OrderNotFoundException")
    void cancel_whenOrderDoesNotExist_shouldThrowOrderNotFoundException() {
        // given
        when(repository.findById(7L)).thenReturn(Optional.empty());

        // when / then
        assertThatThrownBy(() -> tested.cancel(7L, new CancelOrderRequest(null)))
                .isInstanceOf(OrderNotFoundException.class);
    }

    private static OrderEntity newOrder() {
        return withId(new OrderEntity(42L, "customer@example.com", "EUR",
                List.of(new OrderLineEntity("sku-1", 2, new BigDecimal("10.50"))), NOW.minusSeconds(3600)), 7L);
    }

    private static OrderEntity withId(OrderEntity order, Long id) {
        ReflectionTestUtils.setField(order, "id", id);
        return order;
    }
}

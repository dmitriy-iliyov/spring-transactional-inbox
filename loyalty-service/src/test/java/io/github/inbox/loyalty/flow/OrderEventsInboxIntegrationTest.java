package io.github.inbox.loyalty.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.inbox.contract.EventTypes;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import io.github.inbox.contract.Topics;
import io.github.inbox.loyalty.points.CustomerPointsEntity;
import io.github.inbox.loyalty.points.CustomerPointsRepository;
import io.github.inbox.loyalty.points.PointsService;
import io.github.inbox.loyalty.support.ApplicationIntegrationTest;
import io.github.inbox.loyalty.support.Containers;
import io.github.inbox.loyalty.support.OrderEventSender;
import io.github.inbox.loyalty.support.RecordingKafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.inbox.loyalty.support.Await.awaitAtMost;
import static io.github.inbox.loyalty.support.Await.awaitWithin;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Order events sent to Kafka as the outbox sends them, consumed through the transactional inbox.
 * Every case uses customers and orders of its own, so cases do not see each other's points; counters on
 * {@code /actuator/prometheus} are shared by all cases, so they are compared as deltas.
 */
@AutoConfigureMockMvc
@AutoConfigureObservability(tracing = false)
class OrderEventsInboxIntegrationTest extends ApplicationIntegrationTest {

    private static final AtomicLong IDS = new AtomicLong(1_000);
    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    @MockitoSpyBean
    private PointsService pointsService;

    private static final String CONSUMED = "inbox_events_total{type=\"consumed\"}";
    private static final String DUPLICATES = "inbox_events_total{type=\"rejected_duplicates\"}";
    private static final String POINTS_ACCRUED = "loyalty_points_accrued_total";

    private final CustomerPointsRepository balances;
    private final JdbcTemplate jdbc;
    private final MockMvc mockMvc;
    private final OrderEventSender sender;
    private RecordingKafkaConsumer deadLetters;

    OrderEventsInboxIntegrationTest(CustomerPointsRepository balances,
                                    JdbcTemplate jdbc,
                                    MockMvc mockMvc,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper) {
        this.balances = balances;
        this.jdbc = jdbc;
        this.mockMvc = mockMvc;
        this.sender = new OrderEventSender(kafkaTemplate, objectMapper);
    }

    @BeforeEach
    void setUp() {
        deadLetters = new RecordingKafkaConsumer(Containers.KAFKA.getBootstrapServers(), Topics.ORDER_EVENTS + ".DLT");
    }

    @AfterEach
    void tearDown() {
        deadLetters.close();
        reset(pointsService);
    }

    @Test
    @DisplayName("IT order-created when consumed should accrue points and record the event id in the inbox")
    void orderCreated_whenConsumed_shouldAccruePointsAndRecordEventId() {
        // given
        long customerId = IDS.incrementAndGet();
        UUID eventId = UUID.randomUUID();

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, created(IDS.incrementAndGet(), customerId, "21.50"));

        // then
        awaitBalance(customerId, 21);
        assertThat(isInInbox(eventId)).isTrue();
    }

    @Test
    @DisplayName("IT order-created when delivered twice under one id should accrue points once")
    void orderCreated_whenDeliveredTwice_shouldAccrueOnce() {
        // given
        long customerId = IDS.incrementAndGet();
        UUID eventId = UUID.randomUUID();
        String samePartition = String.valueOf(customerId);
        OrderCreatedEvent event = created(IDS.incrementAndGet(), customerId, "30.00");
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        awaitBalance(customerId, 30);

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        // consumed after the repeat, so once it is counted the repeat has been handled too
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition,
                created(IDS.incrementAndGet(), customerId, "5.00"));

        // then
        awaitBalance(customerId, 35);
        assertThat(ledgerEntries(customerId)).isEqualTo(2);
    }

    @Test
    @DisplayName("IT prometheus when an event is redelivered should count one duplicate and accrue its points once")
    void prometheus_whenEventIsRedelivered_shouldCountDuplicateAndAccrueOnce() throws Exception {
        // given
        long customerId = IDS.incrementAndGet();
        UUID eventId = UUID.randomUUID();
        String samePartition = String.valueOf(customerId);
        OrderCreatedEvent event = created(IDS.incrementAndGet(), customerId, "30.00");
        String before = scrape();

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        // consumed after the repeat, so once it is counted the repeat has been handled too
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition,
                created(IDS.incrementAndGet(), customerId, "5.00"));

        // then
        awaitAtMost().untilAsserted(() -> {
            String after = scrape();
            assertThat(delta(before, after, CONSUMED)).as(CONSUMED).isEqualTo(2.0);
            assertThat(delta(before, after, DUPLICATES)).as(DUPLICATES).isEqualTo(1.0);
            assertThat(delta(before, after, POINTS_ACCRUED)).as(POINTS_ACCRUED).isEqualTo(35.0);
        });
    }

    @Test
    @DisplayName("IT order-created when its handling fails once should roll the inbox record back and accrue on retry")
    void orderCreated_whenHandlingFailsOnce_shouldRollBackAndAccrueOnRetry() {
        // given
        long customerId = IDS.incrementAndGet();
        UUID eventId = UUID.randomUUID();
        doThrow(new IllegalStateException("database hiccup"))
                .doCallRealMethod()
                .when(pointsService).accrue(any());

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, created(IDS.incrementAndGet(), customerId, "12.00"));

        // then
        awaitBalance(customerId, 12);
        verify(pointsService, atLeast(2)).accrue(any());
        assertThat(isInInbox(eventId)).isTrue();
        assertThat(ledgerEntries(customerId)).isEqualTo(1);
    }

    @Test
    @DisplayName("IT order-cancelled when it arrives before order-created should leave the balance at zero")
    void orderCancelled_whenArrivesBeforeCreated_shouldLeaveBalanceAtZero() {
        // given
        long customerId = IDS.incrementAndGet();
        long orderId = IDS.incrementAndGet();
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CANCELLED,
                new OrderCancelledEvent(orderId, customerId, "Ordered by mistake", NOW));
        awaitAtMost().until(() -> ledgerEntries(customerId) == 1);

        // when
        UUID createdId = UUID.randomUUID();
        sender.send(createdId, EventTypes.ORDER_CREATED, created(orderId, customerId, "40.00"));

        // then
        awaitAtMost().until(() -> isInInbox(createdId));
        assertThat(balances.findById(customerId)).hasValueSatisfying(
                balance -> assertThat(balance.getBalance()).isZero());
    }

    @Test
    @DisplayName("IT order-cancelled when the order earned points should take them back")
    void orderCancelled_whenOrderEarnedPoints_shouldTakeThemBack() {
        // given
        long customerId = IDS.incrementAndGet();
        long orderId = IDS.incrementAndGet();
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, created(orderId, customerId, "25.00"));
        awaitBalance(customerId, 25);

        // when
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CANCELLED, new OrderCancelledEvent(orderId, customerId, null, NOW));

        // then
        awaitBalance(customerId, 0);
    }

    @Test
    @DisplayName("IT event of an unknown type when consumed should land on the dead letter topic, not in the inbox")
    void event_whenTypeIsUnknown_shouldLandOnDeadLetterTopic() {
        // given
        UUID eventId = UUID.randomUUID();

        // when
        sender.send(eventId, "order-shipped", created(IDS.incrementAndGet(), IDS.incrementAndGet(), "1.00"));

        // then
        awaitAtMost().until(() -> deadLetters.findByHeader("oncebox_event_id", eventId.toString()), Optional::isPresent);
        assertThat(isInInbox(eventId)).isFalse();
    }

    @Test
    @DisplayName("IT event without an event id when consumed should go to the dead letter topic without retries")
    void event_whenEventIdIsMissing_shouldLandOnDeadLetterTopicWithoutRetries() {
        // given
        long orderId = IDS.incrementAndGet();

        // when
        sender.sendWithoutEventId(EventTypes.ORDER_CREATED, created(orderId, IDS.incrementAndGet(), "3.00"));

        // then - the retries alone would take more than 15 seconds
        awaitWithin(Duration.ofSeconds(8)).until(() -> deadLetters.findByPayloadField("orderId", orderId),
                Optional::isPresent);
        assertThat(ledgerEntriesOfOrder(orderId)).isZero();
    }

    @Test
    @DisplayName("IT order-created when replayed after the inbox forgot its id should change nothing and not be parked")
    void orderCreated_whenReplayedAfterInboxForgotIt_shouldChangeNothing() {
        // given
        long customerId = IDS.incrementAndGet();
        String samePartition = String.valueOf(customerId);
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(IDS.incrementAndGet(), customerId, "18.00");
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        awaitBalance(customerId, 18);
        jdbc.update("DELETE FROM inbox_events WHERE id = ?", eventId);

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        // consumed after the replay, so once it is counted the replay has been handled too
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition,
                created(IDS.incrementAndGet(), customerId, "2.00"));

        // then
        awaitBalance(customerId, 20);
        assertThat(ledgerEntries(customerId)).isEqualTo(2);
        assertThat(isInInbox(eventId)).as("the replay is recorded again").isTrue();
        assertThat(deadLetters.findByHeader("oncebox_event_id", eventId.toString())).isEmpty();
    }

    private void awaitBalance(long customerId, long expected) {
        awaitAtMost().until(() -> balances.findById(customerId).map(CustomerPointsEntity::getBalance),
                balance -> balance.isPresent() && balance.get() == expected);
    }

    private boolean isInInbox(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM inbox_events WHERE id = ?", Long.class, eventId) == 1;
    }

    private long ledgerEntriesOfOrder(long orderId) {
        return jdbc.queryForObject("SELECT count(*) FROM points_ledger WHERE order_id = ?", Long.class, orderId);
    }

    private long ledgerEntries(long customerId) {
        return jdbc.queryForObject("SELECT count(*) FROM points_ledger WHERE customer_id = ?", Long.class, customerId);
    }

    private String scrape() throws Exception {
        return mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
    }

    private static double delta(String before, String after, String series) {
        return value(after, series) - value(before, series);
    }

    // a series not written yet reads as zero, as Prometheus treats a counter that has not appeared
    private static double value(String scrape, String series) {
        return scrape.lines()
                .filter(line -> line.startsWith(series + " "))
                .mapToDouble(line -> Double.parseDouble(line.substring(series.length() + 1).trim()))
                .findFirst()
                .orElse(0.0);
    }

    private static OrderCreatedEvent created(long orderId, long customerId, String total) {
        return new OrderCreatedEvent(orderId, customerId, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 1, new BigDecimal(total))), new BigDecimal(total), "EUR", NOW);
    }
}

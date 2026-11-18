package io.github.inbox.notification.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.inbox.contract.EventTypes;
import io.github.inbox.contract.OrderCancelledEvent;
import io.github.inbox.contract.OrderCreatedEvent;
import io.github.inbox.contract.OrderLine;
import io.github.inbox.contract.Topics;
import io.github.inbox.notification.support.ApplicationIntegrationTest;
import io.github.inbox.notification.support.Containers;
import io.github.inbox.notification.support.MailServer;
import io.github.inbox.notification.support.OrderEventSender;
import io.github.inbox.notification.support.RecordingKafkaConsumer;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.inbox.notification.support.Await.awaitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Order events sent to Kafka as the outbox sends them, consumed through the at-most-once inbox. Every case mails a
 * recipient of its own, since the SMTP server is shared.
 */
class OrderConfirmationInboxIntegrationTest extends ApplicationIntegrationTest {

    private static final AtomicLong IDS = new AtomicLong(1_000);
    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    @MockitoSpyBean
    private JavaMailSender mailSender;

    private final JdbcTemplate jdbc;
    private final OrderEventSender sender;
    private final KafkaListenerEndpointRegistry registry;
    private RecordingKafkaConsumer deadLetters;

    OrderConfirmationInboxIntegrationTest(JdbcTemplate jdbc,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          ObjectMapper objectMapper,
                                          KafkaListenerEndpointRegistry registry) {
        this.jdbc = jdbc;
        this.sender = new OrderEventSender(kafkaTemplate, objectMapper);
        this.registry = registry;
    }

    @BeforeEach
    void setUp() {
        deadLetters = new RecordingKafkaConsumer(Containers.KAFKA.getBootstrapServers(), Topics.ORDER_EVENTS + ".DLT");
    }

    @AfterEach
    void tearDown() {
        if (deadLetters != null) {
            deadLetters.close();
        }
        reset(mailSender);
    }

    @Test
    @DisplayName("IT order-created when sending fails once should not send it on redelivery")
    void orderCreated_whenSendingFailsOnce_shouldNotSendOnRedelivery() {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        String marker = "marker-%d@example.com".formatted(orderId);
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(orderId, recipient);

        doThrow(new MailSendException("mail server refused"))
                .doCallRealMethod()
                .when(mailSender).send(any(SimpleMailMessage.class));

        // when - the failing event, then a valid one on the same partition
        sender.send(eventId, EventTypes.ORDER_CREATED, recipient, event);
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, recipient, created(IDS.incrementAndGet(), marker));

        // then - the marker's confirmation proves the listener recovered
        awaitAtMost().until(() -> MailServer.receivedBy(marker).size() == 1);

        assertThat(MailServer.receivedBy(recipient)).isEmpty();
        assertThat(notificationsOf(eventId)).isZero();
        assertThat(isInInbox(eventId)).isTrue();
    }

    @Test
    @DisplayName("IT order-created when consumed should send one confirmation and record it")
    void orderCreated_whenConsumed_shouldSendOneConfirmation() throws Exception {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        UUID eventId = UUID.randomUUID();

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, created(orderId, recipient));

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(recipient).size() == 1);
        MimeMessage mail = MailServer.receivedBy(recipient).getFirst();
        assertThat(mail.getSubject()).isEqualTo("Your order %d is confirmed".formatted(orderId));
        awaitAtMost().until(() -> notificationsOf(eventId) == 1);
        assertThat(isInInbox(eventId)).isTrue();
    }

    @Test
    @DisplayName("IT order-created when delivered twice under one id should send the confirmation once")
    void orderCreated_whenDeliveredTwice_shouldSendOnce() {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        String marker = "marker-%d@example.com".formatted(orderId);
        String samePartition = recipient;
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(orderId, recipient);
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        awaitAtMost().until(() -> MailServer.receivedBy(recipient).size() == 1);

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        // consumed after the repeat, so once it is mailed the repeat has been handled too
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition, created(IDS.incrementAndGet(), marker));

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(marker).size() == 1);
        assertThat(MailServer.receivedBy(recipient)).hasSize(1);
        assertThat(notificationsOf(eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName("IT order-created when duplicate event id within one batch should send the confirmation once")
    void orderCreated_whenDuplicateEventIdWithinOneBatch_shouldSendOnce() {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        String marker = "marker-%d@example.com".formatted(orderId);
        String samePartition = recipient;
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(orderId, recipient);

        pauseListener();
        try {
            sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
            sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
            sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition,
                    created(IDS.incrementAndGet(), marker));
        } finally {
            resumeListener();
        }

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(marker).size() == 1);
        assertThat(MailServer.receivedBy(recipient)).hasSize(1);
        assertThat(notificationsOf(eventId)).isEqualTo(1);
        assertThat(isInInbox(eventId)).isTrue();
    }

    @Test
    @DisplayName("IT order-created when duplicate event id across two batches should send the confirmation once")
    void orderCreated_whenDuplicateEventIdAcrossTwoBatches_shouldSendOnce() {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        String marker = "marker-%d@example.com".formatted(orderId);
        String samePartition = recipient;
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(orderId, recipient);

        // batch 1
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        awaitAtMost().until(() -> MailServer.receivedBy(recipient).size() == 1);

        // batch 2
        sender.send(eventId, EventTypes.ORDER_CREATED, samePartition, event);
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, samePartition,
                created(IDS.incrementAndGet(), marker));

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(marker).size() == 1);
        assertThat(MailServer.receivedBy(recipient)).hasSize(1);
        assertThat(notificationsOf(eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName("IT order-created when malformed record in middle of batch should mail valid ones and route bad to DLT")
    void orderCreated_whenMalformedRecordInMiddleOfBatch_shouldMailValidAndRouteBadToDlt() {
        // given
        long orderId1 = IDS.incrementAndGet();
        long badOrderId = IDS.incrementAndGet();
        long orderId2 = IDS.incrementAndGet();

        String recipient1 = "customer-%d@example.com".formatted(orderId1);
        String recipient2 = "customer-%d@example.com".formatted(orderId2);
        String samePartition = recipient1;

        UUID eventId1 = UUID.randomUUID();
        UUID eventId2 = UUID.randomUUID();

        OrderCreatedEvent event1 = created(orderId1, recipient1);
        OrderCreatedEvent badEvent = created(badOrderId, "bad@example.com");
        OrderCreatedEvent event2 = created(orderId2, recipient2);

        pauseListener();
        try {
            sender.send(eventId1, EventTypes.ORDER_CREATED, samePartition, event1);
            sender.sendWithoutEventId(EventTypes.ORDER_CREATED, samePartition, badEvent);
            sender.send(eventId2, EventTypes.ORDER_CREATED, samePartition, event2);
        } finally {
            resumeListener();
        }

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(recipient1).size() == 1);
        awaitAtMost().until(() -> MailServer.receivedBy(recipient2).size() == 1);
        assertThat(MailServer.receivedBy(recipient1)).hasSize(1);
        assertThat(MailServer.receivedBy(recipient2)).hasSize(1);

        awaitAtMost().until(() -> deadLetters.findByPayloadField("orderId", badOrderId), Optional::isPresent);
        assertThat(deadLetters.findByHeader("oncebox_event_id", eventId1.toString())).isEmpty();
        assertThat(deadLetters.findByHeader("oncebox_event_id", eventId2.toString())).isEmpty();

        assertThat(isInInbox(eventId1)).isTrue();
        assertThat(isInInbox(eventId2)).isTrue();
        assertThat(notificationsOf(eventId1)).isEqualTo(1);
        assertThat(notificationsOf(eventId2)).isEqualTo(1);
    }

    @Test
    @DisplayName("IT order-cancelled when consumed should record the event and send nothing")
    void orderCancelled_whenConsumed_shouldSendNothing() {
        // given
        long orderId = IDS.incrementAndGet();
        UUID eventId = UUID.randomUUID();

        // when
        sender.send(eventId, EventTypes.ORDER_CANCELLED, new OrderCancelledEvent(orderId, 42L, null, NOW));

        // then
        awaitAtMost().until(() -> isInInbox(eventId));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM notifications WHERE order_id = ?", Long.class, orderId))
                .isZero();
    }

    @Test
    @DisplayName("IT order-created when replayed after the inbox forgot its id should not mail the confirmation again")
    void orderCreated_whenReplayedAfterInboxForgotIt_shouldNotMailAgain() {
        // given
        long orderId = IDS.incrementAndGet();
        String recipient = "customer-%d@example.com".formatted(orderId);
        String marker = "marker-%d@example.com".formatted(orderId);
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = created(orderId, recipient);
        sender.send(eventId, EventTypes.ORDER_CREATED, recipient, event);
        awaitAtMost().until(() -> notificationsOf(eventId) == 1);
        jdbc.update("DELETE FROM inbox_events WHERE id = ?", eventId);

        // when
        sender.send(eventId, EventTypes.ORDER_CREATED, recipient, event);
        // consumed after the replay, so once it is mailed the replay has been handled too
        sender.send(UUID.randomUUID(), EventTypes.ORDER_CREATED, recipient, created(IDS.incrementAndGet(), marker));

        // then
        awaitAtMost().until(() -> MailServer.receivedBy(marker).size() == 1);
        assertThat(MailServer.receivedBy(recipient)).hasSize(1);
        assertThat(notificationsOf(eventId)).isEqualTo(1);
    }

    private void pauseListener() {
        registry.getListenerContainers().forEach(MessageListenerContainer::pause);
        awaitAtMost().until(() -> registry.getListenerContainers().stream().allMatch(MessageListenerContainer::isContainerPaused));
    }

    private void resumeListener() {
        registry.getListenerContainers().forEach(MessageListenerContainer::resume);
    }

    private long notificationsOf(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM notifications WHERE event_id = ?", Long.class, eventId);
    }

    private boolean isInInbox(UUID eventId) {
        return jdbc.queryForObject("SELECT count(*) FROM inbox_events WHERE id = ?", Long.class, eventId) == 1;
    }

    private static OrderCreatedEvent created(long orderId, String recipient) {
        return new OrderCreatedEvent(orderId, 42L, recipient,
                List.of(new OrderLine("sku-1", 2, new BigDecimal("10.50"))), new BigDecimal("21.00"), "EUR", NOW);
    }
}

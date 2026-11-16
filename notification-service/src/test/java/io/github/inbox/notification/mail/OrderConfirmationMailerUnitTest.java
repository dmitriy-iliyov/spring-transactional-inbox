package io.github.inbox.notification.mail;

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
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderConfirmationMailerUnitTest {

    private static final UUID EVENT_ID = UUID.fromString("0195f0a0-0000-7000-8000-000000000001");
    private static final Instant NOW = Instant.parse("2026-11-14T10:00:00Z");

    @Mock
    private JavaMailSender mailSender;

    @Mock
    private NotificationRepository notifications;

    private SimpleMeterRegistry meters;
    private OrderConfirmationMailer tested;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        tested = new OrderConfirmationMailer(mailSender, notifications, new MailProperties("orders@example.com"),
                new NotificationMetrics(meters), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("UT sendConfirmation() when the mail is sent should address it to the customer and record it")
    void sendConfirmation_whenMailIsSent_shouldRecordNotification() {
        // when
        tested.sendConfirmation(EVENT_ID, order());

        // then
        ArgumentCaptor<SimpleMailMessage> mail = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(mail.capture());
        assertThat(mail.getValue().getTo()).containsExactly("jane.doe@example.com");
        assertThat(mail.getValue().getFrom()).isEqualTo("orders@example.com");
        assertThat(mail.getValue().getSubject()).isEqualTo("Your order 7 is confirmed");
        assertThat(mail.getValue().getText()).contains("2 x sku-1 at 10.50 EUR", "Total: 21.00 EUR");
        ArgumentCaptor<NotificationEntity> recorded = ArgumentCaptor.forClass(NotificationEntity.class);
        verify(notifications).save(recorded.capture());
        assertThat(recorded.getValue().getEventId()).isEqualTo(EVENT_ID);
        assertThat(recorded.getValue().getSentAt()).isEqualTo(NOW);
        assertThat(count(NotificationMetrics.NOTIFICATIONS_SENT)).isEqualTo(1.0);
        assertThat(count(NotificationMetrics.NOTIFICATIONS_FAILED)).isZero();
    }

    @Test
    @DisplayName("UT sendConfirmation() when the mail server refuses should record nothing")
    void sendConfirmation_whenMailServerRefuses_shouldRecordNothing() {
        // given
        doThrow(new MailSendException("connection refused")).when(mailSender).send(any(SimpleMailMessage.class));

        // when / then
        assertThatThrownBy(() -> tested.sendConfirmation(EVENT_ID, order()))
                .isInstanceOf(MailSendException.class);
        verify(notifications, never()).save(any());
        assertThat(count(NotificationMetrics.NOTIFICATIONS_FAILED)).isEqualTo(1.0);
        assertThat(count(NotificationMetrics.NOTIFICATIONS_SENT)).isZero();
    }

    @Test
    @DisplayName("UT sendConfirmation() when the event was already mailed should send nothing")
    void sendConfirmation_whenEventWasAlreadyMailed_shouldSendNothing() {
        // given
        when(notifications.existsByEventId(EVENT_ID)).thenReturn(true);

        // when
        tested.sendConfirmation(EVENT_ID, order());

        // then
        verifyNoInteractions(mailSender);
        verify(notifications, never()).save(any());
        assertThat(count(NotificationMetrics.NOTIFICATIONS_SENT)).isZero();
    }

    private double count(String meter) {
        return meters.get(meter).counter().count();
    }

    private static OrderCreatedEvent order() {
        return new OrderCreatedEvent(7L, 42L, "jane.doe@example.com",
                List.of(new OrderLine("sku-1", 2, new BigDecimal("10.50"))), new BigDecimal("21.00"), "EUR", NOW);
    }
}

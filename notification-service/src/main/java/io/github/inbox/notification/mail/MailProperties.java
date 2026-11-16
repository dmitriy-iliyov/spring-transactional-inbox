package io.github.inbox.notification.mail;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @param from the sender address of every e-mail the service sends
 */
@ConfigurationProperties(prefix = "notification.mail")
@Validated
public record MailProperties(@NotBlank @Email String from) {
}

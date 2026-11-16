package io.github.inbox.notification.mail;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.validation.FieldError;

import static org.assertj.core.api.Assertions.assertThat;

class MailPropertiesIntegrationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(MailConfiguration.class);

    @ParameterizedTest
    @ValueSource(strings = {"orders@example.com", "notifications+orders@example.org"})
    @DisplayName("IT context when the sender address is valid should bind it unchanged")
    void context_whenSenderAddressIsValid_shouldBindItUnchanged(String address) {
        contextRunner.withPropertyValues("notification.mail.from=" + address).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(MailProperties.class);
            assertThat(context.getBean(MailProperties.class).from()).isEqualTo(address);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    @DisplayName("IT context when the sender address is blank should fail naming the property")
    void context_whenSenderAddressIsBlank_shouldFailNamingTheProperty(String address) {
        assertInvalidSender(contextRunner.withPropertyValues("notification.mail.from=" + address), "NotBlank");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.example.com", "orders@", "@example.com"})
    @DisplayName("IT context when the sender address is malformed should fail naming the property")
    void context_whenSenderAddressIsMalformed_shouldFailNamingTheProperty(String address) {
        assertInvalidSender(contextRunner.withPropertyValues("notification.mail.from=" + address), "Email");
    }

    @Test
    @DisplayName("IT context when the sender address is missing should fail naming the property")
    void context_whenSenderAddressIsMissing_shouldFailNamingTheProperty() {
        assertInvalidSender(contextRunner, "NotBlank");
    }

    private void assertInvalidSender(ApplicationContextRunner runner, String constraint) {
        runner.run(context -> {
            assertThat(context).hasFailed();
            Throwable failure = context.getStartupFailure();
            assertThat(failure).hasRootCauseInstanceOf(BindValidationException.class);
            BindValidationException validation = (BindValidationException)
                    NestedExceptionUtils.getMostSpecificCause(failure);
            assertThat(validation.getValidationErrors().getAllErrors()).anySatisfy(error ->
                    assertThat(error).isInstanceOfSatisfying(FieldError.class, fieldError -> {
                        assertThat(validation.getValidationErrors().getName() + "." + fieldError.getField())
                                .isEqualTo("notification.mail.from");
                        assertThat(fieldError.getCode()).isEqualTo(constraint);
                    }));
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(MailProperties.class)
    static class MailConfiguration {
    }
}

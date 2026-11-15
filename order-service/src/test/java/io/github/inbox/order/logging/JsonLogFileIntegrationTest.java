package io.github.inbox.order.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.FileAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.marker.Markers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.mock.env.MockEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ACTION;
import static io.github.inbox.contract.ObservabilityVocabulary.EVENT_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Configures logback from {@code logback-spring.xml} the way Spring Boot does at startup, but with the properties of
 * each case, and reads back what it wrote. Logback is configured once per JVM, so every case re-initializes it and
 * the default configuration is put back afterwards for the tests that follow.
 */
class JsonLogFileIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(JsonLogFileIntegrationTest.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final LoggingSystem loggingSystem = LoggingSystem.get(getClass().getClassLoader());

    @AfterEach
    void tearDown() {
        initializeLogging(new MockEnvironment().withProperty("spring.application.name", "order-service"));
    }

    @Test
    @DisplayName("IT JSON file when enabled should write service identity, MDC fields and marker fields on each line")
    void jsonFile_whenEnabled_shouldWriteServiceIdentityMdcAndMarkers() throws IOException {
        // given
        initializeLogging(new MockEnvironment()
                .withProperty("spring.application.name", "order-service")
                .withProperty("app.version", "1.2.3")
                .withProperty("app.instance-id", "order-service-test-1")
                .withProperty("app.environment", "test")
                .withProperty("app.logging.json.enabled", "true")
                .withProperty("app.logging.json.path", "target/json-log-test"));

        // when
        try (MDC.MDCCloseable ignored = MDC.putCloseable(EVENT_ID, "0195f0a0-0000-7000-8000-000000000001")) {
            log.info(Markers.append(EVENT_ACTION, "order.created"), "json line under test");
        }

        // then
        JsonNode line = lineWithMessage("json line under test");
        assertThat(line.get("service.name").asText()).isEqualTo("order-service");
        assertThat(line.get("service.version").asText()).isEqualTo("1.2.3");
        assertThat(line.get("instance.id").asText()).isEqualTo("order-service-test-1");
        assertThat(line.get("deployment.environment").asText()).isEqualTo("test");
        assertThat(line.get("log.level").asText()).isEqualTo("INFO");
        assertThat(line.get("@timestamp").asText()).isNotBlank();
        assertThat(line.get(EVENT_ID).asText()).isEqualTo("0195f0a0-0000-7000-8000-000000000001");
        assertThat(line.get(EVENT_ACTION).asText()).isEqualTo("order.created");
    }

    @Test
    @DisplayName("IT JSON file when not enabled should log to the console only")
    void jsonFile_whenNotEnabled_shouldNotBeConfigured() {
        // when
        initializeLogging(new MockEnvironment().withProperty("spring.application.name", "order-service"));

        // then
        assertThat(rootLogger().getAppender("JSON_FILE")).isNull();
        assertThat(rootLogger().getAppender("CONSOLE")).isNotNull();
    }

    private void initializeLogging(MockEnvironment environment) {
        loggingSystem.cleanUp();
        loggingSystem.beforeInitialize();
        loggingSystem.initialize(new LoggingInitializationContext(environment), "classpath:logback-spring.xml", null);
    }

    private static JsonNode lineWithMessage(String message) throws IOException {
        FileAppender<?> appender = (FileAppender<?>) rootLogger().getAppender("JSON_FILE");
        assertThat(appender).as("JSON_FILE appender on the root logger").isNotNull();
        List<String> lines = Files.readAllLines(Path.of(appender.getFile()));
        for (int i = lines.size() - 1; i >= 0; i--) {
            JsonNode line = JSON.readTree(lines.get(i));
            if (message.equals(line.path("message").asText())) {
                return line;
            }
        }
        throw new AssertionError("No JSON line with message '%s' in %s".formatted(message, appender.getFile()));
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(Logger.ROOT_LOGGER_NAME);
    }
}

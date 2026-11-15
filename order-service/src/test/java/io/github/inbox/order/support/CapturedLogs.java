package io.github.inbox.order.support;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.logstash.logback.encoder.LogstashEncoder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records what one class logs, each line rendered as the JSON a log pipeline receives: the message, the MDC of the
 * moment it was written and the fields of its markers. Close it to detach.
 */
public final class CapturedLogs extends AppenderBase<ILoggingEvent> implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Logger logger;
    private final LogstashEncoder encoder = new LogstashEncoder();
    private final List<JsonNode> lines = new CopyOnWriteArrayList<>();

    private CapturedLogs(Class<?> type) {
        this.logger = (Logger) LoggerFactory.getLogger(type);
        setContext(logger.getLoggerContext());
        encoder.setContext(logger.getLoggerContext());
        encoder.start();
        start();
        logger.addAppender(this);
    }

    public static CapturedLogs of(Class<?> type) {
        return new CapturedLogs(type);
    }

    public List<JsonNode> lines() {
        return List.copyOf(lines);
    }

    @Override
    protected void append(ILoggingEvent event) {
        try {
            lines.add(JSON.readTree(encoder.encode(event)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        logger.detachAppender(this);
        stop();
        encoder.stop();
    }
}

package io.github.inbox.contract;

import java.util.List;

/**
 * Field names and {@code event.action} values of the structured logs, shared by every service so that one query
 * finds an order, a customer or an event in the logs of all of them.
 * <p>
 * A field name is the MDC key and the JSON field at once. An action marks the step of the flow a line was written
 * at; log pipelines and saved searches filter on these exact strings.
 */
public final class ObservabilityVocabulary {

    public static final String CORRELATION_ID = "correlationId";
    public static final String CUSTOMER_ID = "customerId";
    public static final String ORDER_ID = "orderId";
    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String EVENT_ACTION = "event.action";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_PARTITION = "kafka.partition";
    public static final String KAFKA_OFFSET = "kafka.offset";

    public static final String ORDER_CREATED = "order.created";
    public static final String ORDER_CANCELLED = "order.cancelled";

    public static final String INBOX_EVENT_CONSUMED = "inbox.event.consumed";
    public static final String INBOX_EVENT_SKIPPED = "inbox.event.skipped";
    public static final String INBOX_EVENT_FAILED = "inbox.event.failed";

    public static final String POINTS_ACCRUED = "points.accrued";
    public static final String POINTS_REVERTED = "points.reverted";

    public static final List<String> FIELDS = List.of(
            CORRELATION_ID, CUSTOMER_ID, ORDER_ID, EVENT_ID, EVENT_TYPE, EVENT_ACTION,
            KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
    );

    public static final List<String> ORDER_SERVICE_ACTIONS = List.of(ORDER_CREATED, ORDER_CANCELLED);

    /**
     * Actions every consumer writes around the inbox: the event was handled, was a repeat and skipped, or failed
     * and went back to the error handler.
     */
    public static final List<String> INBOX_ACTIONS = List.of(
            INBOX_EVENT_CONSUMED, INBOX_EVENT_SKIPPED, INBOX_EVENT_FAILED
    );

    public static final List<String> LOYALTY_SERVICE_ACTIONS = List.of(POINTS_ACCRUED, POINTS_REVERTED);

    private ObservabilityVocabulary() {
    }
}

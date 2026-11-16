package io.github.inbox.order.metrics;

import io.github.inbox.order.support.ApplicationIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static io.github.inbox.order.support.Await.awaitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@AutoConfigureObservability(tracing = false)
class PrometheusEndpointIntegrationTest extends ApplicationIntegrationTest {

    private final MockMvc mockMvc;

    PrometheusEndpointIntegrationTest(MockMvc mockMvc) {
        this.mockMvc = mockMvc;
    }

    @Test
    @DisplayName("IT prometheus when an order is created and sent should expose the order and outbox meters")
    void prometheus_whenOrderIsCreatedAndSent_shouldExposeOrderAndOutboxMeters() throws Exception {
        // given
        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId": 42, "customerEmail": "jane.doe@example.com", "currency": "EUR",
                                 "lines": [{"productId": "sku-1", "quantity": 1, "unitPrice": 10.00}]}
                                """))
                .andExpect(status().isCreated());

        // when / then
        awaitAtMost().untilAsserted(() -> assertThat(scrape())
                .contains("orders_placed_total")
                .containsPattern("outbox_events_rate_total\\{[^}]*event_type=\"order-created\"")
                .contains("outbox_events_by_status")
                .contains("oncebox_task_processing_duration_seconds"));
    }

    @Test
    @DisplayName("IT health when the service is up should answer UP with liveness and readiness groups")
    void health_whenServiceIsUp_shouldAnswerUpWithProbes() throws Exception {
        // when / then
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
    }

    private String scrape() throws Exception {
        return mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
    }
}

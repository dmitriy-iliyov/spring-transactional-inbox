package io.github.inbox.loyalty.logging;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.inbox.contract.ObservabilityVocabulary.CORRELATION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrelationIdFilterUnitTest {

    private final CorrelationIdFilter tested = new CorrelationIdFilter();
    private final MockHttpServletResponse response = new MockHttpServletResponse();
    private final AtomicReference<String> seenByChain = new AtomicReference<>();
    private final FilterChain chain = (request, response) -> seenByChain.set(MDC.get(CORRELATION_ID));

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    @DisplayName("UT doFilter() when the client sends a safe correlation id should log under it and echo it back")
    void doFilter_whenClientSendsSafeId_shouldUseAndEchoIt() throws Exception {
        // given
        MockHttpServletRequest request = request("checkout-2026.11_15-a1");

        // when
        tested.doFilter(request, response, chain);

        // then
        assertThat(seenByChain.get()).isEqualTo("checkout-2026.11_15-a1");
        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo("checkout-2026.11_15-a1");
    }

    @Test
    @DisplayName("UT doFilter() when the client sends no correlation id should generate one and echo it back")
    void doFilter_whenHeaderIsMissing_shouldGenerateUuid() throws Exception {
        // when
        tested.doFilter(new MockHttpServletRequest(), response, chain);

        // then
        assertThat(seenByChain.get()).isNotNull();
        assertThatCode(() -> UUID.fromString(seenByChain.get())).doesNotThrowAnyException();
        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo(seenByChain.get());
    }

    @Test
    @DisplayName("UT doFilter() when the correlation id would break a log line should replace it with a fresh one")
    void doFilter_whenIdHasUnsafeCharacters_shouldReplaceIt() throws Exception {
        // when
        tested.doFilter(request("abc\n2026 ERROR forged line"), response, chain);

        // then
        assertThat(seenByChain.get()).doesNotContain("forged");
        assertThatCode(() -> UUID.fromString(seenByChain.get())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("UT doFilter() when the correlation id is longer than 64 characters should replace it")
    void doFilter_whenIdIsTooLong_shouldReplaceIt() throws Exception {
        // given
        String tooLong = "a".repeat(65);

        // when
        tested.doFilter(request(tooLong), response, chain);

        // then
        assertThat(seenByChain.get()).isNotEqualTo(tooLong);
        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo(seenByChain.get());
    }

    @Test
    @DisplayName("UT doFilter() when the request is served should leave no correlation id behind on the thread")
    void doFilter_whenRequestIsServed_shouldClearMdc() throws Exception {
        // when
        tested.doFilter(request("first"), response, chain);

        // then
        assertThat(MDC.get(CORRELATION_ID)).isNull();
    }

    @Test
    @DisplayName("UT doFilter() when serving the request fails should still clear the correlation id")
    void doFilter_whenChainThrows_shouldClearMdc() {
        // given
        FilterChain failing = (request, response) -> {
            throw new IllegalStateException("boom");
        };

        // when / then
        assertThatThrownBy(() -> tested.doFilter(request("first"), response, failing))
                .isInstanceOf(IllegalStateException.class);
        assertThat(MDC.get(CORRELATION_ID)).isNull();
    }

    private static MockHttpServletRequest request(String correlationId) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationIdFilter.HEADER, correlationId);
        return request;
    }
}

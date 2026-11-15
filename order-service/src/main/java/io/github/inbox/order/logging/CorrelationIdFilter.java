package io.github.inbox.order.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import static io.github.inbox.contract.ObservabilityVocabulary.CORRELATION_ID;

/**
 * Gives every HTTP request a correlation id: in the MDC while the request is served, so every line logged for it
 * carries the id, and in the {@value #HEADER} response header, so the client can quote it.
 * <p>
 * The client's own id is kept when it is at most 64 characters of letters, digits, {@code .}, {@code _} and
 * {@code -}; anything else is replaced with a random UUID, so a header can neither forge nor break a log line.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Correlation-Id";

    private static final Pattern ACCEPTED = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = correlationId(request.getHeader(HEADER));
        response.setHeader(HEADER, correlationId);
        try (MDC.MDCCloseable ignored = MDC.putCloseable(CORRELATION_ID, correlationId)) {
            chain.doFilter(request, response);
        }
    }

    private static String correlationId(String requested) {
        if (requested != null && ACCEPTED.matcher(requested).matches()) {
            return requested;
        }
        return UUID.randomUUID().toString();
    }
}

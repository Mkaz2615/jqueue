package com.jqueue.api.middleware;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.web.filter.OncePerRequestFilter;

import com.jqueue.common.logging.LogContext;
import com.jqueue.common.uid.IdGenerator;

/**
 * Attaches a correlation id to every request.
 *
 * <p>An inbound id is honoured so a trace started by a caller survives the hop; only a
 * request without one gets a fresh id. The id is placed in the logging context, so every
 * log line written while serving the request carries it without any handler passing it
 * around by hand.
 */
public class RequestIdFilter extends OncePerRequestFilter {

    /** The header carrying a correlation id in and out. */
    public static final String HEADER = "X-Request-ID";

    private final IdGenerator ids;

    public RequestIdFilter(IdGenerator ids) {
        this.ids = ids;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String requestId = request.getHeader(HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = ids.newId();
        }
        response.setHeader(HEADER, requestId);
        try (var ignored = LogContext.with("request_id", requestId)) {
            chain.doFilter(request, response);
        }
    }
}

package com.jqueue.api.middleware;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/** Writes one structured line per request. */
public class RequestLoggingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long start = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            String path = request.getRequestURI();
            // Skip the metrics endpoint: a Prometheus scrape every few seconds would
            // otherwise be most of the log volume and none of its value.
            if (!"/metrics".equals(path)) {
                log.atInfo()
                        .addKeyValue("method", request.getMethod())
                        .addKeyValue("path", path)
                        .addKeyValue("status", response.getStatus())
                        .addKeyValue("duration_ms", (System.nanoTime() - start) / 1_000_000)
                        .addKeyValue("client_ip", request.getRemoteAddr())
                        .log("http request");
            }
        }
    }
}

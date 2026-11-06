package com.jqueue.api.handlers;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/** Serves the Prometheus scrape endpoint on the private port. */
@RestController
public class MetricsController {

    private static final MediaType PROMETHEUS_TEXT =
            MediaType.parseMediaType("text/plain; version=0.0.4; charset=utf-8");

    private final PrometheusMeterRegistry registry;

    public MetricsController(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/metrics")
    public ResponseEntity<String> scrape() {
        return ResponseEntity.ok().contentType(PROMETHEUS_TEXT).body(registry.scrape());
    }
}

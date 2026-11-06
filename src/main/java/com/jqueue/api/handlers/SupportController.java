package com.jqueue.api.handlers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.jqueue.application.services.HandlerRegistry;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;
import com.jqueue.common.time.Durations;
import com.jqueue.domain.job.vo.TaskType;

/** Serves liveness, readiness and service metadata. */
@RestController
public class SupportController {

    /** The liveness payload. */
    public record HealthResponse(
            @JsonProperty("status") String status,
            @JsonProperty("service") String service,
            @JsonProperty("version") String version,
            @JsonProperty("uptime") String uptime) {
    }

    /** The readiness payload. */
    public record ReadyResponse(
            @JsonProperty("status") String status,
            @JsonProperty("registered_tasks") List<String> registeredTasks) {
    }

    private final HandlerRegistry registry;
    private final String version;
    private final Instant startedAt = Instant.now();

    public SupportController(
            HandlerRegistry registry,
            @Value("${jqueue.server.version:1.0.0}") String version) {
        this.registry = registry;
        this.version = version;
    }

    /**
     * Liveness answers "is this process alive", so it deliberately checks nothing external.
     * A Redis outage must not make Kubernetes restart every pod: the pods are fine, the
     * dependency is not, and restarting them makes the incident worse.
     */
    @GetMapping("/health")
    public HealthResponse health() {
        Duration uptime = Duration.ofSeconds(Duration.between(startedAt, Instant.now()).toSeconds());
        return new HealthResponse("ok", "jqueue", version, Durations.format(uptime));
    }

    /**
     * Readiness answers "can this process do useful work". A worker with no registered
     * handlers would accept jobs and dead-letter every one of them, so it reports unready
     * and stays out of rotation.
     */
    @GetMapping("/ready")
    public ReadyResponse ready() {
        List<String> names = registry.registeredTypes().stream().map(TaskType::value).toList();
        if (names.isEmpty()) {
            throw AppError.of(ErrorKind.UNAVAILABLE, "no_handlers_registered", "no task handlers are registered");
        }
        return new ReadyResponse("ready", names);
    }
}

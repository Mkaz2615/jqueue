package com.jqueue.common.logging;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.MDC;

/**
 * Attaches structured fields to every log line written on the current thread until the
 * scope is closed.
 *
 * <p>This is the Java counterpart of attaching fields to a request context: anything logged
 * inside the try-with-resources block carries the fields automatically, so no method has to
 * pass a job id around by hand. Closing the scope restores whatever values the keys held
 * before, so nested scopes compose correctly.
 *
 * <pre>{@code
 * try (var ignored = LogContext.with("job_id", id, "queue", queue)) {
 *     log.info("job completed");   // carries job_id and queue
 * }
 * }</pre>
 */
public final class LogContext implements AutoCloseable {

    private final Map<String, String> previous;

    private LogContext(Map<String, String> previous) {
        this.previous = previous;
    }

    /**
     * Opens a scope with alternating key/value arguments. A null value is recorded as an
     * empty string rather than dropped, so the field's presence is predictable.
     */
    public static LogContext with(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("LogContext.with needs key/value pairs");
        }
        Map<String, String> previous = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = String.valueOf(keyValues[i]);
            previous.put(key, MDC.get(key));
            Object value = keyValues[i + 1];
            MDC.put(key, value == null ? "" : String.valueOf(value));
        }
        return new LogContext(previous);
    }

    /** Captures the current thread's fields so they can be replayed on another thread. */
    public static Map<String, String> capture() {
        Map<String, String> copy = MDC.getCopyOfContextMap();
        return copy == null ? Map.of() : copy;
    }

    /** Replaces the current thread's fields with a captured set. */
    public static void restore(Map<String, String> captured) {
        if (captured == null || captured.isEmpty()) {
            MDC.clear();
        } else {
            MDC.setContextMap(captured);
        }
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }
}

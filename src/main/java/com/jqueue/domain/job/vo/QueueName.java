package com.jqueue.domain.job.vo;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * Identifies a logical queue. Jobs in different queues are isolated: they are drained
 * independently and reported on independently.
 */
public record QueueName(String value) implements Comparable<QueueName> {

    /** Used when a caller enqueues without naming a queue. */
    public static final String DEFAULT_NAME = "default";
    public static final QueueName DEFAULT = new QueueName(DEFAULT_NAME);

    /** Characters that are safe inside a Redis key and readable in a Grafana label. */
    private static final Pattern PATTERN = Pattern.compile("^[a-z0-9][a-z0-9_.-]{0,62}$");

    public QueueName {
        Objects.requireNonNull(value, "value");
    }

    /**
     * Validates and constructs a QueueName. An empty input yields the default queue, which
     * keeps the common enqueue path free of ceremony.
     */
    public static QueueName of(String raw) {
        String trimmed = raw == null ? "" : raw.strip().toLowerCase(Locale.ROOT);
        if (trimmed.isEmpty()) {
            return DEFAULT;
        }
        if (!PATTERN.matcher(trimmed).matches()) {
            throw AppError.invalid(JobErrorCodes.INVALID_QUEUE_NAME,
                    "queue name must match ^[a-z0-9][a-z0-9_.-]{0,62}$");
        }
        return new QueueName(trimmed);
    }

    @Override
    public int compareTo(QueueName other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}

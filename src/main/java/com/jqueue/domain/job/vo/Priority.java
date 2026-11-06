package com.jqueue.domain.job.vo;

import java.util.Locale;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * Ranks a job against its siblings inside the same queue. Higher values are dequeued first;
 * jobs of equal priority are dequeued in FIFO order.
 *
 * <p>The range is deliberately small and closed: an open integer range invites callers to
 * invent their own scale and makes the dequeue score arithmetic unbounded.
 */
public record Priority(int level) implements Comparable<Priority> {

    public static final Priority LOW = new Priority(1);
    public static final Priority NORMAL = new Priority(5);
    public static final Priority HIGH = new Priority(8);
    public static final Priority CRITICAL = new Priority(10);

    public static final int MIN_LEVEL = 1;
    public static final int MAX_LEVEL = 10;

    /**
     * Validates and constructs a Priority from a numeric level. Zero is treated as
     * "unspecified" and resolves to {@link #NORMAL}.
     */
    public static Priority of(int level) {
        if (level == 0) {
            return NORMAL;
        }
        if (level < MIN_LEVEL || level > MAX_LEVEL) {
            throw AppError.invalid(JobErrorCodes.INVALID_PRIORITY,
                    "priority must be between " + MIN_LEVEL + " and " + MAX_LEVEL);
        }
        return new Priority(level);
    }

    /**
     * Resolves a named level such as {@code high}. An empty name resolves to
     * {@link #NORMAL} so the field is optional on the wire.
     */
    public static Priority parse(String name) {
        String normalized = name == null ? "" : name.strip().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "" -> NORMAL;
            case "low" -> LOW;
            case "normal", "default" -> NORMAL;
            case "high" -> HIGH;
            case "critical" -> CRITICAL;
            default -> throw AppError.invalid(JobErrorCodes.INVALID_PRIORITY,
                    "priority must be one of low, normal, high, critical");
        };
    }

    @Override
    public int compareTo(Priority other) {
        return Integer.compare(level, other.level);
    }

    /** Renders the closest named level, falling back to the number. */
    @Override
    public String toString() {
        return switch (level) {
            case 1 -> "low";
            case 5 -> "normal";
            case 8 -> "high";
            case 10 -> "critical";
            default -> "p" + level;
        };
    }
}

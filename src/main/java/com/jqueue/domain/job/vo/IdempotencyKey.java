package com.jqueue.domain.job.vo;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * A caller-supplied deduplication token. While a key is held, a second enqueue carrying the
 * same key returns the original job instead of creating a duplicate.
 *
 * <p>The key is optional: {@link #NONE} means "no deduplication".
 */
public record IdempotencyKey(String value) {

    /** Bounds the key so the Redis key space stays bounded. */
    public static final int MAX_LENGTH = 128;

    /** The absent key. */
    public static final IdempotencyKey NONE = new IdempotencyKey("");

    public IdempotencyKey {
        Objects.requireNonNull(value, "value");
    }

    /** Validates and constructs a key. An empty input is legal and yields {@link #NONE}. */
    public static IdempotencyKey of(String raw) {
        String trimmed = raw == null ? "" : raw.strip();
        if (trimmed.isEmpty()) {
            return NONE;
        }
        if (trimmed.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH) {
            throw AppError.invalid(JobErrorCodes.INVALID_IDEMPOTENCY_KEY,
                    "idempotency key must not exceed 128 characters");
        }
        if (JobId.containsAny(trimmed, " \t\n")) {
            throw AppError.invalid(JobErrorCodes.INVALID_IDEMPOTENCY_KEY,
                    "idempotency key must not contain whitespace");
        }
        return new IdempotencyKey(trimmed);
    }

    /** Reports whether deduplication is disabled for the job. */
    public boolean isZero() {
        return value.isEmpty();
    }

    @Override
    public String toString() {
        return value;
    }
}

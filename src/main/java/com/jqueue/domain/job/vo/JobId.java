package com.jqueue.domain.job.vo;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * The immutable identity of a Job aggregate.
 *
 * <p>It is a distinct type rather than a bare string so that the compiler rejects passing a
 * queue name or a task type where an identity is expected. {@link #of(String)} validates
 * untrusted input; the canonical constructor is for values already known to be valid.
 */
public record JobId(String value) {

    /** Bounds the identifier so a caller cannot blow up the Redis key space. */
    public static final int MAX_LENGTH = 64;

    public JobId {
        Objects.requireNonNull(value, "value");
    }

    /** Validates and constructs a JobId. */
    public static JobId of(String raw) {
        String trimmed = raw == null ? "" : raw.strip();
        if (trimmed.isEmpty()) {
            throw AppError.invalid(JobErrorCodes.INVALID_JOB_ID, "job id must not be empty");
        }
        if (trimmed.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH) {
            throw AppError.invalid(JobErrorCodes.INVALID_JOB_ID, "job id must not exceed 64 characters");
        }
        if (containsAny(trimmed, " \t\n{}:")) {
            throw AppError.invalid(JobErrorCodes.INVALID_JOB_ID,
                    "job id must not contain whitespace or reserved characters");
        }
        return new JobId(trimmed);
    }

    /** Reports whether the identity is unset. */
    public boolean isZero() {
        return value.isEmpty();
    }

    @Override
    public String toString() {
        return value;
    }

    static boolean containsAny(String value, String chars) {
        for (int i = 0; i < chars.length(); i++) {
            if (value.indexOf(chars.charAt(i)) >= 0) {
                return true;
            }
        }
        return false;
    }
}

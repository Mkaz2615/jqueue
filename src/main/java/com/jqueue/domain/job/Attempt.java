package com.jqueue.domain.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * An immutable record of one execution of a job.
 *
 * <p>The trail is kept on the aggregate because "why did this job die" is a domain question,
 * not a logging concern.
 *
 * @param number     the 1-based attempt index
 * @param startedAt  when a worker leased the job for this attempt
 * @param finishedAt when the attempt ended, or {@code null} while it is still running
 * @param error      the failure message, empty when the attempt succeeded
 * @param workerId   the worker that ran the attempt
 */
public record Attempt(int number, Instant startedAt, Instant finishedAt, String error, String workerId) {

    public Attempt {
        error = Objects.requireNonNullElse(error, "");
        workerId = Objects.requireNonNullElse(workerId, "");
    }

    /** Returns how long the attempt ran, or zero while it is still open. */
    public Duration duration() {
        if (startedAt == null || finishedAt == null) {
            return Duration.ZERO;
        }
        return Duration.between(startedAt, finishedAt);
    }

    /** Reports whether the attempt has been closed. */
    public boolean isFinished() {
        return finishedAt != null;
    }

    /** Returns a closed copy of this attempt. */
    Attempt finish(Instant at, String errorMessage) {
        return new Attempt(number, startedAt, at, errorMessage, workerId);
    }
}

package com.jqueue.domain.job;

import java.time.Instant;

/**
 * Tells the caller what the domain decided after a failure.
 *
 * @param shouldRetry true when the job was moved to the retrying state
 * @param retryAt     the instant of the next attempt; {@code null} unless retrying
 * @param attempt     the attempt number that just failed
 */
public record FailureOutcome(boolean shouldRetry, Instant retryAt, int attempt) {
}

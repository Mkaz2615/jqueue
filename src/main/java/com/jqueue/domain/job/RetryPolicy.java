package com.jqueue.domain.job;

import java.time.Duration;
import java.util.Objects;

import com.jqueue.common.errors.AppError;

/**
 * An immutable value object describing how a job reacts to failure.
 *
 * <p>It owns the decision of <em>whether</em> to retry and <em>when</em>; it delegates the
 * shape of the waiting curve to a {@link BackoffStrategy}.
 */
public final class RetryPolicy {

    /*
     * Defaults are deliberately conservative: five attempts over roughly a minute of
     * exponential backoff covers a transient dependency blip without holding a worker
     * slot hostage.
     */
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final Duration DEFAULT_BASE_DELAY = Duration.ofSeconds(1);
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(5);
    public static final int MAX_ALLOWED_RETRIES = 100;
    public static final Duration MAX_ALLOWED_BASE_DELAY = Duration.ofHours(1);

    private static final RetryPolicy DEFAULT = new RetryPolicy(
            DEFAULT_MAX_RETRIES, DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY, BackoffStrategy.Exponential.INSTANCE);

    private final int maxRetries;
    private final Duration baseDelay;
    private final Duration maxDelay;
    private final BackoffStrategy strategy;

    private RetryPolicy(int maxRetries, Duration baseDelay, Duration maxDelay, BackoffStrategy strategy) {
        this.maxRetries = maxRetries;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.strategy = strategy;
    }

    /**
     * Validates and constructs a policy. Zero delays fall back to the documented defaults so
     * that callers may omit the whole block.
     */
    public static RetryPolicy of(int maxRetries, Duration baseDelay, Duration maxDelay, String strategyName) {
        Duration base = baseDelay == null ? Duration.ZERO : baseDelay;
        Duration max = maxDelay == null ? Duration.ZERO : maxDelay;

        if (maxRetries < 0 || maxRetries > MAX_ALLOWED_RETRIES) {
            throw AppError.invalid(JobErrorCodes.INVALID_RETRY_POLICY, "max retries must not exceed 100");
        }
        if (base.isNegative() || base.compareTo(MAX_ALLOWED_BASE_DELAY) > 0) {
            throw AppError.invalid(JobErrorCodes.INVALID_RETRY_POLICY, "base delay must be between 0 and 1h");
        }
        if (base.isZero()) {
            base = DEFAULT_BASE_DELAY;
        }
        if (max.isZero()) {
            max = DEFAULT_MAX_DELAY;
        }
        if (max.compareTo(base) < 0) {
            throw AppError.invalid(JobErrorCodes.INVALID_RETRY_POLICY,
                    "max delay must be greater than or equal to base delay");
        }
        return new RetryPolicy(maxRetries, base, max, BackoffStrategy.of(strategyName));
    }

    /** The policy applied when a caller supplies none. */
    public static RetryPolicy defaults() {
        return DEFAULT;
    }

    /** The number of retries allowed after the first attempt. */
    public int maxRetries() {
        return maxRetries;
    }

    /** The unit delay fed to the strategy. */
    public Duration baseDelay() {
        return baseDelay;
    }

    /** The ceiling applied to every computed delay. */
    public Duration maxDelay() {
        return maxDelay;
    }

    /** The persisted strategy identifier. */
    public String strategyName() {
        return strategy.name();
    }

    /** Reports whether a job attempted {@code attemptsMade} times may be retried again. */
    public boolean allowsRetry(int attemptsMade) {
        return attemptsMade <= maxRetries;
    }

    /** Returns how long to wait before the given retry attempt. */
    public Duration nextDelay(int attempt) {
        return strategy.delay(attempt, baseDelay, maxDelay);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RetryPolicy other
                && maxRetries == other.maxRetries
                && baseDelay.equals(other.baseDelay)
                && maxDelay.equals(other.maxDelay)
                && strategyName().equals(other.strategyName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxRetries, baseDelay, maxDelay, strategyName());
    }

    @Override
    public String toString() {
        return "RetryPolicy[maxRetries=" + maxRetries + ", baseDelay=" + baseDelay
                + ", maxDelay=" + maxDelay + ", strategy=" + strategyName() + "]";
    }
}

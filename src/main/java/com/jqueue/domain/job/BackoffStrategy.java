package com.jqueue.domain.job;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import com.jqueue.common.errors.AppError;

/**
 * Computes how long to wait before attempt number {@code attempt} (1-based: attempt 1 is
 * the first retry).
 *
 * <p>It is an interface so that a new waiting curve can be added without editing
 * {@link RetryPolicy} or the processor: the open/closed principle applied to the one piece
 * of retry behaviour that genuinely varies between workloads.
 */
public interface BackoffStrategy {

    String EXPONENTIAL = "exponential";
    String LINEAR = "linear";
    String CONSTANT = "constant";

    /** Identifies the strategy for persistence and for the dashboard. */
    String name();

    /** Returns the wait before the given attempt, clamped to {@code [0, maxDelay]}. */
    Duration delay(int attempt, Duration base, Duration maxDelay);

    /**
     * Resolves a persisted strategy name back to its behaviour. An empty name resolves to
     * exponential, which is the documented default.
     */
    static BackoffStrategy of(String name) {
        String normalized = name == null ? "" : name.strip().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "", EXPONENTIAL -> Exponential.INSTANCE;
            case LINEAR -> Linear.INSTANCE;
            case CONSTANT -> Constant.INSTANCE;
            default -> throw AppError.invalid(JobErrorCodes.INVALID_RETRY_POLICY,
                    "backoff strategy must be one of exponential, linear, constant");
        };
    }

    /**
     * Doubles the base delay on every attempt and adds jitter.
     *
     * <p>Jitter matters in a distributed queue: without it, a downstream outage synchronises
     * every failed job onto the same retry instant and the recovering dependency is hit by
     * the entire backlog at once.
     */
    final class Exponential implements BackoffStrategy {
        public static final Exponential INSTANCE = new Exponential();

        private Exponential() {
        }

        @Override
        public String name() {
            return EXPONENTIAL;
        }

        /** Returns {@code base * 2^(attempt-1)} with jitter, clamped to {@code maxDelay}. */
        @Override
        public Duration delay(int attempt, Duration base, Duration maxDelay) {
            int effective = Math.max(attempt, 1);
            // Cap the exponent before multiplying so the arithmetic cannot overflow.
            double exponent = Math.min(effective - 1, 32);
            double baseNanos = base.toNanos();
            double scaled = baseNanos * Math.pow(2, exponent);
            double maxNanos = maxDelay.toNanos();
            if (scaled > maxNanos) {
                scaled = maxNanos;
            }
            // Sample uniformly from [base, scaled] so retries spread out while never
            // dropping below the configured floor.
            if (scaled <= baseNanos) {
                return clamp(base, maxDelay);
            }
            double jittered = baseNanos + ThreadLocalRandom.current().nextDouble() * (scaled - baseNanos);
            return clamp(Duration.ofNanos((long) jittered), maxDelay);
        }
    }

    /** Grows the delay by {@code base} on every attempt. */
    final class Linear implements BackoffStrategy {
        public static final Linear INSTANCE = new Linear();

        private Linear() {
        }

        @Override
        public String name() {
            return LINEAR;
        }

        @Override
        public Duration delay(int attempt, Duration base, Duration maxDelay) {
            int effective = Math.max(attempt, 1);
            return clamp(base.multipliedBy(effective), maxDelay);
        }
    }

    /** Waits the same amount before every attempt. */
    final class Constant implements BackoffStrategy {
        public static final Constant INSTANCE = new Constant();

        private Constant() {
        }

        @Override
        public String name() {
            return CONSTANT;
        }

        @Override
        public Duration delay(int attempt, Duration base, Duration maxDelay) {
            return clamp(base, maxDelay);
        }
    }

    /** Keeps a computed delay inside {@code [0, maxDelay]}. */
    private static Duration clamp(Duration delay, Duration maxDelay) {
        if (delay.isNegative()) {
            return Duration.ZERO;
        }
        if (maxDelay.isPositive() && delay.compareTo(maxDelay) > 0) {
            return maxDelay;
        }
        return delay;
    }
}

package com.jqueue.domain.job;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RetryPolicyTest {

    @Test
    void appliesDefaultsForZeroValues() {
        RetryPolicy policy = RetryPolicy.of(0, Duration.ZERO, Duration.ZERO, "");

        assertThat(policy.maxRetries()).isZero();
        assertThat(policy.baseDelay()).isEqualTo(RetryPolicy.DEFAULT_BASE_DELAY);
        assertThat(policy.maxDelay()).isEqualTo(RetryPolicy.DEFAULT_MAX_DELAY);
        assertThat(policy.strategyName()).isEqualTo(BackoffStrategy.EXPONENTIAL);
    }

    @Test
    void rejectsInconsistentDelays() {
        assertCode(() -> RetryPolicy.of(3, Duration.ofMinutes(1), Duration.ofSeconds(1), BackoffStrategy.LINEAR),
                JobErrorCodes.INVALID_RETRY_POLICY);
    }

    @Test
    void rejectsAnUnknownStrategy() {
        assertCode(() -> RetryPolicy.of(3, Duration.ofSeconds(1), Duration.ofMinutes(1), "fibonacci"),
                JobErrorCodes.INVALID_RETRY_POLICY);
    }

    @Test
    void rejectsTooManyRetries() {
        assertCode(() -> RetryPolicy.of(101, Duration.ofSeconds(1), Duration.ofMinutes(1), ""),
                JobErrorCodes.INVALID_RETRY_POLICY);
    }

    @Test
    void allowsRetryUpToTheBudget() {
        RetryPolicy policy = RetryPolicy.of(2, Duration.ofSeconds(1), Duration.ofMinutes(1), BackoffStrategy.CONSTANT);

        assertThat(policy.allowsRetry(1)).isTrue();
        assertThat(policy.allowsRetry(2)).isTrue();
        assertThat(policy.allowsRetry(3)).isFalse();
    }

    @Test
    void exponentialBackoff_growsAndStaysWithinBounds() {
        BackoffStrategy strategy = BackoffStrategy.of(BackoffStrategy.EXPONENTIAL);
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(30);

        // Jitter makes each delay a sample rather than a fixed value, so the useful
        // assertions are the bounds, not an exact number. Asserting on a specific jittered
        // value would produce a test that fails one run in a thousand.
        for (int attempt = 1; attempt <= 10; attempt++) {
            Duration delay = strategy.delay(attempt, base, max);
            assertThat(delay).as("attempt %d", attempt).isBetween(base, max);
        }
    }

    @Test
    void exponentialBackoff_doesNotOverflowAtExtremeAttempts() {
        // 2^1000 would overflow a naive implementation and produce a negative or infinite
        // duration; the exponent is clamped precisely to stop that.
        Duration delay = BackoffStrategy.of(BackoffStrategy.EXPONENTIAL)
                .delay(1000, Duration.ofSeconds(1), Duration.ofHours(1));

        assertThat(delay).isPositive().isLessThanOrEqualTo(Duration.ofHours(1));
    }

    @Test
    void linearBackoff_growsByOneBaseDelayPerAttempt() {
        BackoffStrategy strategy = BackoffStrategy.of(BackoffStrategy.LINEAR);

        assertThat(strategy.delay(1, Duration.ofSeconds(1), Duration.ofMinutes(1))).isEqualTo(Duration.ofSeconds(1));
        assertThat(strategy.delay(5, Duration.ofSeconds(1), Duration.ofMinutes(1))).isEqualTo(Duration.ofSeconds(5));
        // Clamped at the ceiling.
        assertThat(strategy.delay(120, Duration.ofSeconds(1), Duration.ofMinutes(1))).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void constantBackoff_ignoresTheAttemptNumber() {
        BackoffStrategy strategy = BackoffStrategy.of(BackoffStrategy.CONSTANT);

        assertThat(strategy.delay(1, Duration.ofSeconds(5), Duration.ofMinutes(1))).isEqualTo(Duration.ofSeconds(5));
        assertThat(strategy.delay(99, Duration.ofSeconds(5), Duration.ofMinutes(1))).isEqualTo(Duration.ofSeconds(5));
    }

    @ParameterizedTest
    @CsvSource({
        "'',            exponential",
        "exponential,   exponential",
        "linear,        linear",
        "constant,      constant",
        "EXPONENTIAL,   exponential",
    })
    void backoffStrategy_resolvesEveryRegisteredName(String input, String expected) {
        assertThat(BackoffStrategy.of(input).name()).isEqualTo(expected);
    }
}

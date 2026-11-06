package com.jqueue.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;
import com.jqueue.common.time.Durations;
import com.jqueue.hydrator.JobHydrator;

class CommonTest {

    @ParameterizedTest
    @CsvSource({
        "0,          0s",
        "500,        500ms",
        "1000,       1s",
        "1500,       1.5s",
        "90000,      1m30s",
        "3600000,    1h0m0s",
        "86400000,   24h0m0s",
    })
    void durationsFormatCompactly(long millis, String expected) {
        assertThat(Durations.format(Duration.ofMillis(millis))).isEqualTo(expected);
    }

    @Test
    void timestampsUseTheShortestExactFraction() {
        assertThat(JobHydrator.formatTime(null)).isEmpty();
        assertThat(JobHydrator.formatTime(Instant.parse("2026-01-15T10:00:00Z"))).isEqualTo("2026-01-15T10:00:00Z");
        assertThat(JobHydrator.formatTime(Instant.parse("2026-01-15T10:00:00.120Z"))).isEqualTo("2026-01-15T10:00:00.12Z");
    }

    @Test
    void appErrorRendersCodeMessageAndCause() {
        AppError plain = AppError.invalid("invalid_priority", "priority must be between 1 and 10");
        AppError wrapped = AppError.internal("job_save_failed", "failed to save job", new IllegalStateException("conn reset"));

        assertThat(plain.getMessage()).isEqualTo("invalid_priority: priority must be between 1 and 10");
        assertThat(wrapped.getMessage()).isEqualTo("job_save_failed: failed to save job: conn reset");
        assertThat(wrapped.safeMessage()).isEqualTo("failed to save job");
        assertThat(AppError.kindOf(new RuntimeException(wrapped))).isEqualTo(ErrorKind.INTERNAL);
        assertThat(AppError.find(new RuntimeException(plain))).contains(plain);
        assertThat(AppError.describe(new NullPointerException())).isEqualTo("NullPointerException");
    }
}

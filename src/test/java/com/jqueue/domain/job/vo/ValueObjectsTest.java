package com.jqueue.domain.job.vo;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.jqueue.domain.job.JobErrorCodes;

class ValueObjectsTest {

    @Test
    void jobId_rejectsEmptyOverlongAndReservedInput() {
        assertCode(() -> JobId.of("  "), JobErrorCodes.INVALID_JOB_ID);
        assertCode(() -> JobId.of("x".repeat(65)), JobErrorCodes.INVALID_JOB_ID);
        assertCode(() -> JobId.of("has space"), JobErrorCodes.INVALID_JOB_ID);
        // Braces and colons would let a caller forge a Redis key or a hash tag.
        assertCode(() -> JobId.of("a:b"), JobErrorCodes.INVALID_JOB_ID);
        assertCode(() -> JobId.of("{tag}"), JobErrorCodes.INVALID_JOB_ID);
        assertThat(JobId.of("  job-1 ").value()).isEqualTo("job-1");
    }

    @Test
    void queueName_defaultsAndNormalises() {
        assertThat(QueueName.of("")).isEqualTo(QueueName.DEFAULT);
        assertThat(QueueName.of(" Critical ").value()).isEqualTo("critical");
        assertCode(() -> QueueName.of("-leading-dash"), JobErrorCodes.INVALID_QUEUE_NAME);
        assertCode(() -> QueueName.of("has space"), JobErrorCodes.INVALID_QUEUE_NAME);
    }

    @Test
    void taskType_allowsDottedAndColonNames() {
        assertThat(TaskType.of("email.welcome").value()).isEqualTo("email.welcome");
        assertThat(TaskType.of("image:resize").value()).isEqualTo("image:resize");
        assertCode(() -> TaskType.of(""), JobErrorCodes.INVALID_TASK_TYPE);
        assertCode(() -> TaskType.of(".leading-dot"), JobErrorCodes.INVALID_TASK_TYPE);
    }

    @Test
    void priority_parsesNamesAndBoundsLevels() {
        assertThat(Priority.parse("")).isEqualTo(Priority.NORMAL);
        assertThat(Priority.parse("default")).isEqualTo(Priority.NORMAL);
        assertThat(Priority.parse("CRITICAL")).isEqualTo(Priority.CRITICAL);
        assertThat(Priority.of(0)).isEqualTo(Priority.NORMAL);
        assertThat(Priority.of(3).toString()).isEqualTo("p3");
        assertCode(() -> Priority.parse("urgent"), JobErrorCodes.INVALID_PRIORITY);
        assertCode(() -> Priority.of(11), JobErrorCodes.INVALID_PRIORITY);
    }

    @Test
    void idempotencyKey_isOptionalButBounded() {
        assertThat(IdempotencyKey.of(null).isZero()).isTrue();
        assertThat(IdempotencyKey.of("order-42").value()).isEqualTo("order-42");
        assertCode(() -> IdempotencyKey.of("x".repeat(129)), JobErrorCodes.INVALID_IDEMPOTENCY_KEY);
        assertCode(() -> IdempotencyKey.of("two words"), JobErrorCodes.INVALID_IDEMPOTENCY_KEY);
    }

    @Test
    void jobState_encodesTheLifecycleGraph() {
        assertThat(JobState.fromWire("pending")).isEqualTo(JobState.PENDING);
        assertCode(() -> JobState.fromWire("zombie"), JobErrorCodes.INVALID_JOB_STATE);

        assertThat(JobState.PENDING.canTransitionTo(JobState.ACTIVE)).isTrue();
        assertThat(JobState.DEAD.canTransitionTo(JobState.PENDING)).isTrue();
        // Nothing can move a job from completed back to pending.
        assertThat(JobState.COMPLETED.canTransitionTo(JobState.PENDING)).isFalse();
        assertThat(JobState.SCHEDULED.canTransitionTo(JobState.ACTIVE)).isFalse();
    }
}

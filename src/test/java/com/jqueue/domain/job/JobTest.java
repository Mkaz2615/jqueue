package com.jqueue.domain.job;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;

class JobTest {

    /** A fixed instant, so every scheduling assertion is exact rather than "roughly now". */
    static final Instant BASE = Instant.parse("2026-01-15T10:00:00Z");

    /**
     * Builds a valid job, letting each test override only what it cares about. Without this
     * every test would repeat ten fields of setup and the one line that matters would be
     * lost in them.
     */
    @SafeVarargs
    static Job newTestJob(Consumer<Job.NewJobParams>... mutations) {
        Job.NewJobParams params = Job.NewJobParams.builder()
                .id("job-1")
                .queue(QueueName.DEFAULT)
                .taskType("email.send")
                .payload("{\"to\":\"a@b.c\"}".getBytes(StandardCharsets.UTF_8))
                .priority(Priority.NORMAL)
                .retryPolicy(RetryPolicy.defaults())
                .timeout(Duration.ofSeconds(30))
                .now(BASE);
        for (Consumer<Job.NewJobParams> m : mutations) {
            m.accept(params);
        }
        return params.build();
    }

    static RetryPolicy constant(int maxRetries) {
        return RetryPolicy.of(maxRetries, Duration.ofSeconds(1), Duration.ofMinutes(1), BackoffStrategy.CONSTANT);
    }

    @Test
    void newJob_startsPendingWhenDueImmediately() {
        Job job = newTestJob();

        assertThat(job.state()).isEqualTo(JobState.PENDING);
        assertThat(job.processAt()).isEqualTo(BASE);
        assertThat(job.attemptsMade()).isZero();
    }

    @Test
    void newJob_startsScheduledWhenDueLater() {
        Instant runAt = BASE.plus(Duration.ofHours(1));
        Job job = newTestJob(p -> p.processAt(runAt));

        assertThat(job.state()).isEqualTo(JobState.SCHEDULED);
        assertThat(job.processAt()).isEqualTo(runAt);
    }

    @Test
    void newJob_rejectsOversizedPayload() {
        assertCode(() -> Job.NewJobParams.builder()
                .id("job-1")
                .taskType("email.send")
                .payload(new byte[Job.MAX_PAYLOAD_BYTES + 1])
                .now(BASE)
                .build(), JobErrorCodes.INVALID_PAYLOAD);
    }

    @Test
    void newJob_defensivelyCopiesPayload() {
        byte[] payload = "{\"secret\":true}".getBytes(StandardCharsets.UTF_8);
        Job job = newTestJob(p -> p.payload(payload));

        // Mutating the caller's array must not reach inside the aggregate, or an
        // aggregate's state could change without any method being called on it.
        payload[2] = 'X';

        assertThat(new String(job.payload(), StandardCharsets.UTF_8)).isEqualTo("{\"secret\":true}");
    }

    @Test
    void lease_recordsAttemptAndExpiry() {
        Job job = newTestJob();

        job.lease("worker-1", BASE, Duration.ofSeconds(10));

        assertThat(job.state()).isEqualTo(JobState.ACTIVE);
        assertThat(job.attemptsMade()).isEqualTo(1);
        assertThat(job.workerId()).isEqualTo("worker-1");
        assertThat(job.attempts()).hasSize(1);
        assertThat(job.attempts().getFirst().number()).isEqualTo(1);
        assertThat(job.attempts().getFirst().workerId()).isEqualTo("worker-1");
        // timeout (30s) + grace (10s)
        assertThat(job.leaseExpiry()).isEqualTo(BASE.plusSeconds(40));
    }

    @Test
    void lease_rejectsAnAlreadyActiveJob() {
        Job job = newTestJob();
        job.lease("worker-1", BASE, Duration.ofSeconds(1));

        // This is the invariant that stops two workers holding the same job.
        assertCode(() -> job.lease("worker-2", BASE, Duration.ofSeconds(1)), JobErrorCodes.ILLEGAL_STATE_TRANSITION);
        assertThat(job.workerId()).isEqualTo("worker-1");
    }

    @Test
    void complete_closesTheOpenAttempt() {
        Job job = newTestJob();
        job.lease("worker-1", BASE, Duration.ofSeconds(1));

        Instant finishedAt = BASE.plusSeconds(2);
        job.complete(finishedAt);

        assertThat(job.state()).isEqualTo(JobState.COMPLETED);
        assertThat(job.state().isTerminal()).isTrue();
        assertThat(job.completedAt()).isEqualTo(finishedAt);
        assertThat(job.leaseExpiry()).isNull();
        assertThat(job.attempts().getFirst().duration()).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void complete_rejectsAJobThatWasNeverLeased() {
        Job job = newTestJob();

        assertCode(() -> job.complete(BASE), JobErrorCodes.ILLEGAL_STATE_TRANSITION);
    }

    @Test
    void fail_schedulesARetryWhileBudgetRemains() {
        Job job = newTestJob(p -> p.retryPolicy(constant(3)));
        job.lease("worker-1", BASE, Duration.ofSeconds(1));

        FailureOutcome outcome = job.fail(new RuntimeException("smtp timeout"), BASE.plusSeconds(1));

        assertThat(outcome.shouldRetry()).isTrue();
        assertThat(outcome.attempt()).isEqualTo(1);
        assertThat(job.state()).isEqualTo(JobState.RETRYING);
        assertThat(job.lastError()).isEqualTo("smtp timeout");
        // Constant backoff, so the retry lands exactly one base delay later.
        assertThat(outcome.retryAt()).isEqualTo(BASE.plusSeconds(2));
        assertThat(job.remainingRetries()).isEqualTo(2);
    }

    @Test
    void fail_movesToDeadWhenRetriesAreExhausted() {
        Job job = newTestJob(p -> p.retryPolicy(constant(1)));

        // Attempt 1 fails and retries; attempt 2 fails and exhausts the budget.
        job.lease("worker-1", BASE, Duration.ofSeconds(1));
        FailureOutcome first = job.fail(new RuntimeException("boom"), BASE);
        assertThat(first.shouldRetry()).isTrue();

        job.promote(BASE);
        job.lease("worker-1", BASE, Duration.ofSeconds(1));
        FailureOutcome second = job.fail(new RuntimeException("boom again"), BASE);

        assertThat(second.shouldRetry()).isFalse();
        assertThat(job.state()).isEqualTo(JobState.DEAD);
        assertThat(job.diedAt()).isNotNull();
        assertThat(job.remainingRetries()).isZero();
        assertThat(job.attempts()).hasSize(2);
    }

    @Test
    void fail_rejectsAJobThatIsNotActive() {
        Job job = newTestJob();

        assertCode(() -> job.fail(new RuntimeException("boom"), BASE), JobErrorCodes.ILLEGAL_STATE_TRANSITION);
    }

    @Test
    void requeue_resetsTheRetryBudget() {
        Job job = newTestJob(p -> p.retryPolicy(constant(0)));
        job.lease("worker-1", BASE, Duration.ofSeconds(1));
        job.fail(new RuntimeException("boom"), BASE);
        assertThat(job.state()).isEqualTo(JobState.DEAD);

        Instant requeuedAt = BASE.plus(Duration.ofHours(1));
        job.requeue(requeuedAt);

        assertThat(job.state()).isEqualTo(JobState.PENDING);
        assertThat(job.attemptsMade()).isZero();
        assertThat(job.diedAt()).isNull();
        assertThat(job.workerId()).isEmpty();
        assertThat(job.processAt()).isEqualTo(requeuedAt);
    }

    @Test
    void requeue_rejectsAJobThatIsNotDead() {
        Job job = newTestJob();

        assertCode(() -> job.requeue(BASE), JobErrorCodes.JOB_NOT_RETRYABLE);
    }

    @Test
    void reclaimExpiredLease_returnsAnOrphanedJobToPending() {
        Job job = newTestJob();
        job.lease("worker-1", BASE, Duration.ofSeconds(10));

        // The lease is timeout (30s) + grace (10s) = 40s from BASE.
        assertThat(job.isLeaseExpired(BASE.plusSeconds(39))).isFalse();
        assertThat(job.isLeaseExpired(BASE.plusSeconds(41))).isTrue();

        job.reclaimExpiredLease(BASE.plusSeconds(41));

        assertThat(job.state()).isEqualTo(JobState.PENDING);
        assertThat(job.workerId()).isEmpty();
        assertThat(job.leaseExpiry()).isNull();
        assertThat(job.lastError()).contains("lease expired");
        // The attempt is kept and stays counted: the worker really did try once, so an
        // orphaned job must not get an unlimited retry budget.
        assertThat(job.attemptsMade()).isEqualTo(1);
        assertThat(job.attempts().getFirst().finishedAt()).isNotNull();
    }

    @Test
    void kill_skipsRemainingRetries() {
        Job job = newTestJob();
        job.lease("worker-1", BASE, Duration.ofSeconds(1));

        job.kill("killed by operator", BASE);

        assertThat(job.state()).isEqualTo(JobState.DEAD);
        assertThat(job.lastError()).isEqualTo("killed by operator");
        assertThat(job.diedAt()).isNotNull();
    }

    @Test
    void kill_isRejectedForACompletedJob() {
        Job job = newTestJob();
        job.lease("worker-1", BASE, Duration.ofSeconds(1));
        job.complete(BASE);

        // Nothing can move a job out of completed: the lifecycle table refuses.
        assertCode(() -> job.kill("too late", BASE), JobErrorCodes.ILLEGAL_STATE_TRANSITION);
    }

    @Test
    void isDue() {
        Job job = newTestJob(p -> p.processAt(BASE.plus(Duration.ofMinutes(1))));

        assertThat(job.isDue(BASE)).isFalse();
        assertThat(job.isDue(BASE.plus(Duration.ofMinutes(1)))).isTrue();
        assertThat(job.isDue(BASE.plus(Duration.ofMinutes(2)))).isTrue();
    }

    @Test
    void copy_isIndependentOfTheOriginal() {
        Job original = newTestJob();
        Job copy = original.copy();

        copy.lease("worker-1", BASE, Duration.ofSeconds(1));

        assertThat(original.state()).isEqualTo(JobState.PENDING);
        assertThat(original.attempts()).isEmpty();
        assertThat(copy.state()).isEqualTo(JobState.ACTIVE);
    }
}

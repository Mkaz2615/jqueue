package com.jqueue.application.enqueuer;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jqueue.contracts.job.EnqueueJobRequest;
import com.jqueue.contracts.job.EnqueueJobResponse;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.JobFilter;
import com.jqueue.testing.InMemoryStore;
import com.jqueue.testing.MutableClock;
import com.jqueue.testing.SequenceIdGenerator;

/**
 * The service under test runs against in-memory fakes and a frozen clock. There is no
 * Redis, no network and no sleeping in any test below, which is the point of having the
 * service depend on interfaces.
 */
class DefaultEnqueuerServiceTest {

    static final Instant NOW = Instant.parse("2026-01-15T10:00:00Z");

    InMemoryStore store;
    DefaultEnqueuerService service;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        service = new DefaultEnqueuerService(
                store, store, store,
                new SequenceIdGenerator("job"),
                new MutableClock(NOW),
                new EnqueuerConfig(Duration.ofHours(1)));
    }

    @Test
    void createsAPendingJob() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder()
                .taskType("email.send")
                .queue("critical")
                .priority("high")
                .payloadJson("{\"to\":\"a@b.c\"}")
                .build());

        assertThat(res.id()).isEqualTo("job-1");
        assertThat(res.queue()).isEqualTo("critical");
        assertThat(res.state()).isEqualTo("pending");
        assertThat(res.priority()).isEqualTo("high");
        assertThat(res.deduplicated()).isFalse();

        Job stored = store.findById(new JobId("job-1"));
        assertThat(stored.state()).isEqualTo(JobState.PENDING);
        assertThat(new String(stored.payload(), StandardCharsets.UTF_8)).isEqualTo("{\"to\":\"a@b.c\"}");
    }

    @Test
    void defaultsTheQueueAndPriority() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder().taskType("email.send").build());

        assertThat(res.queue()).isEqualTo(QueueName.DEFAULT_NAME);
        assertThat(res.priority()).isEqualTo("normal");
    }

    @Test
    void schedulesAJobWithADelay() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder()
                .taskType("report.generate")
                .delaySeconds(300L)
                .build());

        assertThat(res.state()).isEqualTo("scheduled");
        assertThat(store.findById(new JobId(res.id())).processAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void schedulesAJobAtAnAbsoluteInstant() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder()
                .taskType("report.generate")
                .runAt(NOW.plus(Duration.ofHours(2)).toString())
                .build());

        assertThat(res.state()).isEqualTo("scheduled");
        assertThat(store.findById(new JobId(res.id())).processAt()).isEqualTo(NOW.plus(Duration.ofHours(2)));
    }

    @Test
    void acceptsAnRfc3339InstantWithAnOffset() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder()
                .taskType("report.generate")
                .runAt("2026-01-15T17:30:00+05:30")
                .build());

        assertThat(store.findById(new JobId(res.id())).processAt()).isEqualTo(NOW.plus(Duration.ofHours(2)));
    }

    @Test
    void rejectsBothSchedulingFieldsAtOnce() {
        // Silently preferring one would make the API lie about what it did.
        assertCode(() -> service.enqueue(EnqueueJobRequest.builder()
                .taskType("email.send")
                .delaySeconds(60L)
                .runAt(NOW.plus(Duration.ofHours(1)).toString())
                .build()), JobErrorCodes.INVALID_SCHEDULE_TIME);
    }

    @Test
    void rejectsAMalformedRunAt() {
        assertCode(() -> service.enqueue(EnqueueJobRequest.builder()
                .taskType("email.send")
                .runAt("tomorrow")
                .build()), JobErrorCodes.INVALID_SCHEDULE_TIME);
    }

    @Test
    void rejectsAnInvalidTaskType() {
        assertCode(() -> service.enqueue(EnqueueJobRequest.builder().taskType("").build()),
                JobErrorCodes.INVALID_TASK_TYPE);
    }

    @Test
    void rejectsAnInvalidBackoffStrategy() {
        assertCode(() -> service.enqueue(EnqueueJobRequest.builder()
                .taskType("email.send")
                .backoffStrategy("fibonacci")
                .build()), JobErrorCodes.INVALID_RETRY_POLICY);
    }

    @Test
    void deduplicatesOnAnIdempotencyKey() {
        EnqueueJobRequest req = EnqueueJobRequest.builder()
                .taskType("email.send")
                .idempotencyKey("welcome-user-42")
                .build();

        EnqueueJobResponse first = service.enqueue(req);
        assertThat(first.deduplicated()).isFalse();

        EnqueueJobResponse second = service.enqueue(req);

        // The second call returns the original job rather than an error, which is what makes
        // the endpoint safe for a client to retry after a timeout.
        assertThat(second.deduplicated()).isTrue();
        assertThat(second.id()).isEqualTo(first.id());
        assertThat(store.list(JobFilter.all(100)).total()).isEqualTo(1);
    }

    @Test
    void reportsAConflictWhenTheKeyOutlivedItsJob() {
        EnqueueJobRequest req = EnqueueJobRequest.builder()
                .taskType("email.send")
                .idempotencyKey("welcome-user-42")
                .build();
        EnqueueJobResponse first = service.enqueue(req);
        // Simulate the completed job's retention expiring while the key is still held.
        store.delete(new JobId(first.id()));
        store.claim(com.jqueue.domain.job.vo.IdempotencyKey.of("welcome-user-42"), new JobId(first.id()), Duration.ofHours(1));

        assertCode(() -> service.enqueue(req), JobErrorCodes.DUPLICATE_JOB);
    }

    @Test
    void releasesTheClaimWhenPersistenceFails() {
        EnqueueJobRequest req = EnqueueJobRequest.builder()
                .taskType("email.send")
                .idempotencyKey("welcome-user-42")
                .build();

        store.failNext(new IllegalStateException("redis is down"));
        assertThatThrownBy(() -> service.enqueue(req)).hasMessageContaining("redis is down");

        // The key must be free again: a failed enqueue that leaves a claim behind would lock
        // the caller out for the whole TTL over a job that never existed.
        EnqueueJobResponse res = service.enqueue(req);
        assertThat(res.deduplicated()).isFalse();
    }

    @Test
    void appliesACustomRetryPolicy() {
        EnqueueJobResponse res = service.enqueue(EnqueueJobRequest.builder()
                .taskType("demo.flaky")
                .maxRetries(7)
                .retryBaseDelaySeconds(3L)
                .backoffStrategy("linear")
                .timeoutSeconds(45L)
                .build());

        Job stored = store.findById(new JobId(res.id()));
        RetryPolicy policy = stored.retryPolicy();
        assertThat(policy.maxRetries()).isEqualTo(7);
        assertThat(policy.baseDelay()).isEqualTo(Duration.ofSeconds(3));
        assertThat(policy.strategyName()).isEqualTo("linear");
        assertThat(stored.timeout()).isEqualTo(Duration.ofSeconds(45));
    }
}

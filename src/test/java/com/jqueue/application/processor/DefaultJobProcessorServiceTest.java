package com.jqueue.application.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jqueue.application.services.JobContext;
import com.jqueue.application.services.JobHandler;
import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.BackoffStrategy;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.metrics.NopMetricsRecorder;
import com.jqueue.domain.metrics.Outcome;
import com.jqueue.testing.InMemoryStore;

class DefaultJobProcessorServiceTest {

    static final Instant CREATED = Instant.parse("2026-01-15T10:00:00Z");

    /**
     * Captures the measurements the processor emits, so the tests can assert that the
     * dashboard would show the truth — not just that the job ended in the right state.
     */
    static final class SpyRecorder extends NopMetricsRecorder {
        final Map<Outcome, AtomicInteger> processed = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();
        final AtomicInteger retries = new AtomicInteger();
        final List<Duration> durations = new ArrayList<>();

        @Override
        public void recordJobProcessed(QueueName queue, TaskType taskType, Outcome outcome) {
            processed.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void recordJobRetry(QueueName queue, TaskType taskType) {
            retries.incrementAndGet();
        }

        @Override
        public void recordJobFailed(QueueName queue, TaskType taskType, String reason) {
            failures.computeIfAbsent(reason, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public synchronized void recordJobDuration(QueueName queue, TaskType taskType, Duration d) {
            durations.add(d);
        }

        int processed(Outcome outcome) {
            return processed.getOrDefault(outcome, new AtomicInteger()).get();
        }

        int failures(String reason) {
            return failures.getOrDefault(reason, new AtomicInteger()).get();
        }
    }

    InMemoryStore store;
    DefaultHandlerRegistry registry;
    SpyRecorder spy;
    DefaultJobProcessorService service;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        registry = new DefaultHandlerRegistry();
        spy = new SpyRecorder();
        service = new DefaultJobProcessorService(
                registry, store, store, spy, Clock.systemUTC(), new ProcessorConfig(Duration.ofHours(1)));
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    /** A job already leased to a worker, which is the state the processor always receives. */
    static Job leasedJob(String taskType, int maxRetries, Duration timeout) {
        Job job = Job.NewJobParams.builder()
                .id("job-1")
                .queue(QueueName.DEFAULT)
                .taskType(taskType)
                .retryPolicy(RetryPolicy.of(maxRetries, Duration.ofMillis(1), Duration.ofSeconds(1), BackoffStrategy.CONSTANT))
                .timeout(timeout)
                .now(CREATED)
                .build();
        job.lease("worker-1", Instant.now(), Duration.ofSeconds(1));
        return job;
    }

    static Job leasedJob(String taskType, int maxRetries) {
        return leasedJob(taskType, maxRetries, Duration.ofSeconds(1));
    }

    void register(String taskType, JobHandler handler) {
        registry.register(new TaskType(taskType), handler);
    }

    @Test
    void completesASuccessfulJob() {
        register("email.send", job -> {
        });
        Job job = leasedJob("email.send", 3);

        service.process(job);

        assertThat(job.state()).isEqualTo(JobState.COMPLETED);
        assertThat(spy.processed(Outcome.SUCCESS)).isEqualTo(1);
        assertThat(spy.retries.get()).isZero();
        assertThat(spy.durations).hasSize(1);
    }

    @Test
    void retriesAFailedJobWhileBudgetRemains() {
        register("demo.flaky", job -> {
            throw new Exception("downstream unavailable");
        });
        Job job = leasedJob("demo.flaky", 3);

        // A handler failure is an expected outcome, so process does not throw: only a broken
        // broker or store is a processing failure.
        assertThatCode(() -> service.process(job)).doesNotThrowAnyException();

        assertThat(job.state()).isEqualTo(JobState.RETRYING);
        assertThat(job.lastError()).isEqualTo("downstream unavailable");
        assertThat(spy.retries.get()).isEqualTo(1);
        assertThat(spy.processed(Outcome.FAILURE)).isEqualTo(1);
        assertThat(spy.failures("handler_error")).isEqualTo(1);
    }

    @Test
    void labelsAnAppErrorByItsCode() {
        register("email.send", job -> {
            throw AppError.invalid("invalid_payload", "email payload requires a 'to' address");
        });
        Job job = leasedJob("email.send", 3);

        service.process(job);

        assertThat(spy.failures("invalid_payload")).isEqualTo(1);
    }

    @Test
    void deadLettersAJobWithNoRetriesLeft() {
        register("demo.flaky", job -> {
            throw new Exception("still broken");
        });
        Job job = leasedJob("demo.flaky", 0);

        service.process(job);

        assertThat(job.state()).isEqualTo(JobState.DEAD);
        assertThat(spy.processed(Outcome.DEAD)).isEqualTo(1);
        assertThat(spy.retries.get()).isZero();
    }

    @Test
    void deadLettersAnUnroutableJobWithoutRetrying() {
        Job job = leasedJob("unregistered.task", 5);

        service.process(job);

        // Retrying would burn the whole budget waiting for a deploy that may never happen,
        // so an unroutable job goes straight to the dead-letter queue — where an operator
        // who deploys the missing handler can requeue it with a fresh budget.
        assertThat(job.state()).isEqualTo(JobState.DEAD);
        assertThat(spy.retries.get()).isZero();
        assertThat(spy.processed(Outcome.DEAD)).isEqualTo(1);
        assertThat(spy.failures(JobErrorCodes.HANDLER_NOT_FOUND)).isEqualTo(1);
    }

    @Test
    void treatsAHandlerTimeoutAsAFailure() {
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        register("demo.slow", job -> {
            // A well-behaved handler lets interruption propagate; the timeout is what
            // triggers it.
            try {
                Thread.sleep(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            }
        });
        Job job = leasedJob("demo.slow", 2, Duration.ofMillis(50));

        long start = System.nanoTime();
        service.process(job);

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        assertThat(job.state()).isEqualTo(JobState.RETRYING);
        assertThat(job.lastError()).contains("exceeded its 50ms timeout");
        // Labelled separately from a plain handler error, because a timeout and a bug need
        // different fixes.
        assertThat(spy.failures(JobErrorCodes.JOB_TIMEOUT)).isEqualTo(1);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(interrupted::get);
    }

    @Test
    void containsAPanickingHandler() {
        register("demo.panic", job -> {
            throw new IllegalStateException("handler blew up");
        });
        Job job = leasedJob("demo.panic", 2);

        // The point of the assertion is that this call returns at all: without the crash
        // barrier it would take the worker thread down with it.
        assertThatCode(() -> service.process(job)).doesNotThrowAnyException();

        assertThat(job.state()).isEqualTo(JobState.RETRYING);
        assertThat(job.lastError()).contains("handler blew up");
        assertThat(spy.failures(JobErrorCodes.HANDLER_PANIC)).isEqualTo(1);
    }

    @Test
    void passesTheAttemptNumberToTheHandler() {
        AtomicReference<JobContext> seen = new AtomicReference<>();
        register("email.send", seen::set);
        Job job = leasedJob("email.send", 3);

        service.process(job);

        assertThat(seen.get().attempt()).isEqualTo(1);
        assertThat(seen.get().maxRetries()).isEqualTo(3);
        assertThat(seen.get().id()).isEqualTo(new JobId("job-1"));
    }

    @Test
    void persistsTheLeaseBeforeRunningTheHandler() {
        AtomicReference<JobState> storedDuringRun = new AtomicReference<>();
        register("email.send", job -> storedDuringRun.set(store.findById(job.id()).state()));
        Job job = leasedJob("email.send", 3);

        service.process(job);

        // If the process died mid-handler, the stored document must already say "active"
        // so the reclaim sweep can find it.
        assertThat(storedDuringRun.get()).isEqualTo(JobState.ACTIVE);
    }
}

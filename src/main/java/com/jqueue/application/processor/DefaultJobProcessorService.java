package com.jqueue.application.processor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.application.services.HandlerRegistry;
import com.jqueue.application.services.JobContext;
import com.jqueue.application.services.JobHandler;
import com.jqueue.application.services.JobProcessorService;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;
import com.jqueue.common.logging.LogContext;
import com.jqueue.common.time.Durations;
import com.jqueue.domain.job.FailureOutcome;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.domain.metrics.Outcome;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobRepository;

/**
 * Executes one leased job and records its outcome.
 *
 * <p>This is where the reliability guarantees actually land: the timeout, the crash barrier,
 * the retry-or-die decision and the metrics all happen in {@link #process}. The worker pool
 * around it only supplies jobs and threads.
 *
 * <p>Each handler runs on its own virtual thread so its timeout can be enforced from the
 * outside: when it elapses the handler is interrupted and the worker moves on, rather than
 * a stuck handler holding a pool slot until the process restarts.
 */
public final class DefaultJobProcessorService implements JobProcessorService, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultJobProcessorService.class);

    /** Bounded label used when a failure carries no error code of its own. */
    static final String HANDLER_ERROR_REASON = "handler_error";

    private final HandlerRegistry registry;
    private final JobBroker broker;
    private final JobRepository jobRepository;
    private final MetricsRecorder metrics;
    private final Clock clock;
    private final ProcessorConfig config;
    private final ExecutorService handlerExecutor;

    public DefaultJobProcessorService(
            HandlerRegistry registry,
            JobBroker broker,
            JobRepository jobRepository,
            MetricsRecorder metrics,
            Clock clock,
            ProcessorConfig config) {
        this.registry = registry;
        this.broker = broker;
        this.jobRepository = jobRepository;
        this.metrics = metrics;
        this.clock = clock;
        this.config = config;
        this.handlerExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("jqueue-handler-", 0).factory());
    }

    @Override
    public void process(Job job) {
        try (var ignored = LogContext.with(
                "job_id", job.id().value(),
                "queue", job.queue().value(),
                "task_type", job.taskType().value(),
                "attempt", job.attemptsMade())) {

            // Persist the lease the broker handed us before running anything. If the process
            // dies mid-handler, the stored document already shows an active job with a
            // lease, and the reclaim sweep will find it.
            jobRepository.save(job);

            JobHandler handler;
            try {
                handler = registry.resolve(job.taskType());
            } catch (AppError unroutable) {
                // An unroutable job is a configuration problem, not a transient one. Retrying
                // it would burn the whole budget waiting for a deploy that may never come, so
                // it goes straight to the dead-letter queue where an operator can see it.
                kill(job, unroutable);
                return;
            }

            Instant startedAt = clock.instant();
            Throwable handlerError = runHandler(job, handler);
            Duration duration = Duration.between(startedAt, clock.instant());

            metrics.recordJobDuration(job.queue(), job.taskType(), duration);

            if (handlerError == null) {
                complete(job, duration);
            } else {
                fail(job, handlerError, duration);
            }
        }
    }

    /**
     * Executes the handler under the job's timeout, behind a crash barrier, and returns the
     * failure (or {@code null} on success).
     *
     * <p>Both guards exist for the same reason: a worker thread is shared infrastructure. A
     * handler that blocks forever would hold a pool slot until the process restarts, and a
     * handler that crashes must not take the worker down with it. Converting each into an
     * ordinary job failure keeps one bad task type from becoming an outage.
     */
    private Throwable runHandler(Job job, JobHandler handler) {
        JobContext context = new JobContext(
                job.id(), job.queue(), job.taskType(), job.payload(),
                job.attemptsMade(), job.retryPolicy().maxRetries());
        Map<String, String> logFields = LogContext.capture();

        Future<?> execution = handlerExecutor.submit(() -> {
            LogContext.restore(logFields);
            handler.handle(context);
            return null;
        });

        try {
            execution.get(job.timeout().toNanos(), TimeUnit.NANOSECONDS);
            return null;
        } catch (TimeoutException timedOut) {
            // Distinguish "the handler ran out of time" from "the handler failed", because
            // they mean different things to whoever reads the dashboard.
            execution.cancel(true);
            return AppError.of(ErrorKind.INTERNAL, JobErrorCodes.JOB_TIMEOUT,
                    "handler exceeded its " + Durations.format(job.timeout()) + " timeout");
        } catch (ExecutionException failed) {
            return classify(failed.getCause());
        } catch (InterruptedException interrupted) {
            execution.cancel(true);
            Thread.currentThread().interrupt();
            return AppError.of(ErrorKind.INTERNAL, JobErrorCodes.JOB_TIMEOUT,
                    "worker was interrupted while the handler was running");
        }
    }

    /**
     * Separates an expected failure from a crash. A checked exception or an {@code AppError}
     * is the handler reporting a problem; any other unchecked throwable is a bug, and is
     * labelled as such so it is not mistaken for a flaky dependency.
     */
    private Throwable classify(Throwable cause) {
        if (cause instanceof AppError || !(cause instanceof RuntimeException || cause instanceof Error)) {
            return cause;
        }
        AppError crash = AppError.of(ErrorKind.INTERNAL, JobErrorCodes.HANDLER_PANIC,
                "handler panicked: " + AppError.describe(cause));
        log.atError().setCause(cause).addKeyValue("error", crash.getMessage()).log("handler panicked");
        return crash;
    }

    /** Records a successful execution. */
    private void complete(Job job, Duration duration) {
        job.complete(clock.instant());
        broker.complete(job, config.completedRetention());

        metrics.recordJobProcessed(job.queue(), job.taskType(), Outcome.SUCCESS);
        log.atInfo().addKeyValue("duration_ms", duration.toMillis()).log("job completed");
    }

    /**
     * Records a failed execution and lets the aggregate decide what follows.
     *
     * <p>This method contains no retry arithmetic. It asks the job what happened and reacts;
     * the policy itself lives on the aggregate, where it can be tested without a broker and
     * cannot drift between callers.
     */
    private void fail(Job job, Throwable cause, Duration duration) {
        FailureOutcome outcome = job.fail(cause, clock.instant());

        String reason = failureReason(cause);
        metrics.recordJobFailed(job.queue(), job.taskType(), reason);

        if (outcome.shouldRetry()) {
            broker.retry(job, outcome.retryAt());
            metrics.recordJobRetry(job.queue(), job.taskType());
            metrics.recordJobProcessed(job.queue(), job.taskType(), Outcome.FAILURE);

            log.atWarn()
                    .addKeyValue("reason", reason)
                    .addKeyValue("error", AppError.describe(cause))
                    .addKeyValue("retry_at", outcome.retryAt().toString())
                    .addKeyValue("remaining_retries", job.remainingRetries())
                    .addKeyValue("duration_ms", duration.toMillis())
                    .log("job failed, scheduled for retry");
            return;
        }

        broker.kill(job);
        metrics.recordJobProcessed(job.queue(), job.taskType(), Outcome.DEAD);

        log.atError()
                .addKeyValue("reason", reason)
                .addKeyValue("error", AppError.describe(cause))
                .addKeyValue("attempts_made", job.attemptsMade())
                .log("job exhausted its retries and moved to the dead-letter queue");
    }

    /** Moves a job straight to the dead-letter queue. */
    private void kill(Job job, Throwable cause) {
        job.kill(AppError.describe(cause), clock.instant());
        broker.kill(job);

        String reason = failureReason(cause);
        metrics.recordJobFailed(job.queue(), job.taskType(), reason);
        metrics.recordJobProcessed(job.queue(), job.taskType(), Outcome.DEAD);

        log.atError()
                .addKeyValue("reason", reason)
                .addKeyValue("error", AppError.describe(cause))
                .log("job moved to the dead-letter queue without retrying");
    }

    /**
     * Extracts a bounded label for the failure metric.
     *
     * <p>The raw error message must never become a label value: handler errors often embed
     * ids or timestamps, and a label with unbounded cardinality will take the Prometheus
     * server down long before it tells anyone anything useful.
     */
    static String failureReason(Throwable error) {
        return AppError.find(error).map(AppError::code).orElse(HANDLER_ERROR_REASON);
    }

    /** Stops accepting handler executions. In-flight handlers are left to finish. */
    @Override
    public void close() {
        handlerExecutor.shutdown();
    }
}

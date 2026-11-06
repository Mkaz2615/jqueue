package com.jqueue.application.enqueuer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.application.services.EnqueuerService;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;
import com.jqueue.common.uid.IdGenerator;
import com.jqueue.contracts.job.EnqueueJobRequest;
import com.jqueue.contracts.job.EnqueueJobResponse;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.persistence.IdempotencyStore;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobRepository;
import com.jqueue.hydrator.JobHydrator;

/**
 * Turns a client request into a queued job.
 *
 * <p>Its dependencies are all domain interfaces. Nothing here knows that the store is Redis,
 * which is why the whole service is testable with in-memory fakes.
 */
public final class DefaultEnqueuerService implements EnqueuerService {

    private static final Logger log = LoggerFactory.getLogger(DefaultEnqueuerService.class);

    private final JobBroker broker;
    private final IdempotencyStore idempotency;
    private final JobRepository jobRepository;
    private final IdGenerator ids;
    private final Clock clock;
    private final EnqueuerConfig config;

    public DefaultEnqueuerService(
            JobBroker broker,
            IdempotencyStore idempotency,
            JobRepository jobRepository,
            IdGenerator ids,
            Clock clock,
            EnqueuerConfig config) {
        this.broker = broker;
        this.idempotency = idempotency;
        this.jobRepository = jobRepository;
        this.ids = ids;
        this.clock = clock;
        this.config = config;
    }

    /**
     * Validates a request, deduplicates it, and places the job on its queue.
     *
     * <p>The order of operations is deliberate: claim the idempotency key <em>before</em>
     * writing the job. If the write then fails, the claim is released, so a failed enqueue
     * does not poison the key for the next 24 hours. Claiming after the write would leave a
     * window in which two concurrent requests both succeed.
     */
    @Override
    public EnqueueJobResponse enqueue(EnqueueJobRequest request) {
        Job job = buildJob(request);

        if (!job.idempotencyKey().isZero()) {
            IdempotencyStore.ClaimResult claim =
                    idempotency.claim(job.idempotencyKey(), job.id(), config.idempotencyTtl());
            if (!claim.claimed()) {
                return resolveDuplicate(job.idempotencyKey(), claim.existingJobId());
            }
        }

        try {
            persist(job);
        } catch (RuntimeException persistFailure) {
            // Roll the claim back so the caller's next attempt is not rejected as a duplicate
            // of a job that was never created.
            if (!job.idempotencyKey().isZero()) {
                try {
                    idempotency.release(job.idempotencyKey());
                } catch (RuntimeException releaseFailure) {
                    log.atError()
                            .addKeyValue("job_id", job.id().value())
                            .addKeyValue("error", AppError.describe(releaseFailure))
                            .log("failed to release idempotency claim after enqueue failure");
                }
            }
            throw persistFailure;
        }

        log.atInfo()
                .addKeyValue("job_id", job.id().value())
                .addKeyValue("queue", job.queue().value())
                .addKeyValue("task_type", job.taskType().value())
                .addKeyValue("state", job.state().wire())
                .log("job enqueued");

        return JobHydrator.toEnqueueResponse(job, false);
    }

    /** Routes the job to the right broker call for its initial state. */
    private void persist(Job job) {
        if (job.state() == JobState.SCHEDULED) {
            broker.schedule(job);
        } else {
            broker.enqueue(job);
        }
    }

    /**
     * Answers a request whose idempotency key was already held.
     *
     * <p>Returning the original job, rather than an error, is what makes the endpoint safe to
     * retry: a client that times out and resends gets the same job id back and can carry on
     * as if the first call had succeeded.
     */
    private EnqueueJobResponse resolveDuplicate(IdempotencyKey key, JobId existingId) {
        if (existingId == null || existingId.isZero()) {
            throw AppError.conflict(JobErrorCodes.DUPLICATE_JOB,
                    "idempotency key " + key + " is already in use");
        }

        Job existing;
        try {
            existing = jobRepository.findById(existingId);
        } catch (AppError e) {
            // The claim outlived the job it protected, which happens when a completed job's
            // retention expires before the key does. Report the conflict rather than
            // inventing a response for a job that is gone.
            if (e.kind() == ErrorKind.NOT_FOUND) {
                throw AppError.conflict(JobErrorCodes.DUPLICATE_JOB,
                        "idempotency key " + key + " is already in use");
            }
            throw e;
        }

        log.atInfo()
                .addKeyValue("job_id", existing.id().value())
                .addKeyValue("idempotency_key", key.value())
                .log("enqueue deduplicated");

        return JobHydrator.toEnqueueResponse(existing, true);
    }

    /**
     * Translates the request DTO into a validated aggregate.
     *
     * <p>Every field is pushed through its value object rather than validated ad hoc, so the
     * rules a job must satisfy are identical whether it arrived over HTTP or was built by a
     * test.
     */
    private Job buildJob(EnqueueJobRequest req) {
        TaskType taskType = TaskType.of(req.taskType());
        QueueName queue = QueueName.of(req.queue());
        Priority priority = Priority.parse(req.priority());
        IdempotencyKey idempotencyKey = IdempotencyKey.of(req.idempotencyKey());
        JobId id = JobId.of(ids.newId());

        Instant now = clock.instant();
        Instant processAt = resolveProcessAt(req, now);

        RetryPolicy policy = RetryPolicy.of(
                req.maxRetries() == null ? RetryPolicy.DEFAULT_MAX_RETRIES : req.maxRetries(),
                secondsOr(req.retryBaseDelaySeconds(), RetryPolicy.DEFAULT_BASE_DELAY),
                secondsOr(req.retryMaxDelaySeconds(), RetryPolicy.DEFAULT_MAX_DELAY),
                req.backoffStrategy());

        return Job.NewJobParams.builder()
                .id(id)
                .queue(queue)
                .taskType(taskType)
                .payload(req.payloadBytes())
                .priority(priority)
                .retryPolicy(policy)
                .idempotencyKey(idempotencyKey)
                .timeout(secondsOr(req.timeoutSeconds(), Job.DEFAULT_TIMEOUT))
                .processAt(processAt)
                .now(now)
                .build();
    }

    /**
     * Turns the two mutually exclusive scheduling fields into one instant, rejecting a
     * request that sets both rather than silently picking one.
     */
    static Instant resolveProcessAt(EnqueueJobRequest req, Instant now) {
        if (req.delaySeconds() != null && req.runAt() != null) {
            throw AppError.invalid(JobErrorCodes.INVALID_SCHEDULE_TIME,
                    "delay_seconds and run_at are mutually exclusive");
        }
        if (req.delaySeconds() != null) {
            return now.plusSeconds(req.delaySeconds());
        }
        if (req.runAt() != null) {
            try {
                return OffsetDateTime.parse(req.runAt()).toInstant();
            } catch (DateTimeParseException e) {
                throw AppError.invalid(JobErrorCodes.INVALID_SCHEDULE_TIME,
                        "run_at must be a valid RFC3339 timestamp");
            }
        }
        return now;
    }

    /** Reads an optional second count as a Duration with a fallback. */
    private static Duration secondsOr(Long value, Duration fallback) {
        if (value == null || value <= 0) {
            return fallback;
        }
        return Duration.ofSeconds(value);
    }
}

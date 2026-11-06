package com.jqueue.domain.job;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * The aggregate root: the consistency boundary for everything that happens to a single unit
 * of work.
 *
 * <p>Every lifecycle change goes through a method on {@code Job}. Services orchestrate and
 * persist; they never assign to a field directly. That keeps the invariants — legal
 * transitions, attempt accounting, retry exhaustion — in one auditable place instead of
 * scattered across the application layer.
 *
 * <p>Instances are not thread-safe. A repository hands every caller its own copy, so two
 * workers never share one.
 */
public final class Job {

    /** A queue is not a blob store; oversized payloads belong behind a reference. */
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration MAX_TIMEOUT = Duration.ofHours(24);

    /** Recorded on an attempt whose worker vanished without reporting back. */
    public static final String LEASE_EXPIRED_MESSAGE = "lease expired: worker did not report completion";

    private final JobId id;
    private final QueueName queue;
    private final TaskType taskType;
    private final byte[] payload;
    private final Priority priority;
    private final RetryPolicy retryPolicy;
    private final IdempotencyKey idempotencyKey;
    private final Duration timeout;
    private final Instant createdAt;

    private JobState state;
    private int attemptsMade;
    private final List<Attempt> attempts;
    private String lastError;
    private Instant processAt;
    private Instant completedAt;
    private Instant diedAt;
    private Instant leaseExpiry;
    private String workerId;

    private Job(JobSnapshot s) {
        this.id = s.id();
        this.queue = s.queue();
        this.taskType = s.taskType();
        this.payload = s.payload() == null ? new byte[0] : s.payload().clone();
        this.priority = s.priority();
        this.state = s.state();
        this.retryPolicy = s.retryPolicy();
        this.idempotencyKey = s.idempotencyKey() == null ? IdempotencyKey.NONE : s.idempotencyKey();
        this.timeout = s.timeout();
        this.attemptsMade = s.attemptsMade();
        this.attempts = s.attempts() == null ? new ArrayList<>() : new ArrayList<>(s.attempts());
        this.lastError = Objects.requireNonNullElse(s.lastError(), "");
        this.createdAt = s.createdAt();
        this.processAt = s.processAt();
        this.completedAt = s.completedAt();
        this.diedAt = s.diedAt();
        this.leaseExpiry = s.leaseExpiry();
        this.workerId = Objects.requireNonNullElse(s.workerId(), "");
    }

    // ------------------------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------------------------

    /**
     * Validates the parameters and returns a job in its correct initial state: pending when
     * it should run now, scheduled when it should run later.
     */
    public static Job create(NewJobParams p) {
        if (p.id == null || p.id.isZero()) {
            throw AppError.invalid(JobErrorCodes.INVALID_JOB_ID, "job id is required");
        }
        if (p.taskType == null || p.taskType.value().isEmpty()) {
            throw AppError.invalid(JobErrorCodes.INVALID_TASK_TYPE, "task type is required");
        }
        QueueName queue = p.queue == null ? QueueName.DEFAULT : p.queue;
        byte[] payload = p.payload == null ? new byte[0] : p.payload;
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw AppError.invalid(JobErrorCodes.INVALID_PAYLOAD, "payload must not exceed 256KiB");
        }
        Priority priority = p.priority == null ? Priority.NORMAL : p.priority;
        Duration timeout = p.timeout == null || p.timeout.isZero() ? DEFAULT_TIMEOUT : p.timeout;
        if (timeout.isNegative() || timeout.compareTo(MAX_TIMEOUT) > 0) {
            throw AppError.invalid(JobErrorCodes.INVALID_TIMEOUT, "timeout must be between 1ns and 24h");
        }
        if (p.now == null) {
            throw AppError.invalid(JobErrorCodes.INVALID_SCHEDULE_TIME, "creation time is required");
        }

        Instant now = p.now;
        Instant processAt = p.processAt;
        JobState state = JobState.PENDING;
        if (processAt == null || !processAt.isAfter(now)) {
            processAt = now;
        } else {
            state = JobState.SCHEDULED;
        }

        RetryPolicy policy = p.retryPolicy == null ? RetryPolicy.defaults() : p.retryPolicy;

        return new Job(new JobSnapshot(
                p.id, queue, p.taskType, payload, priority, state, policy,
                p.idempotencyKey, timeout, 0, List.of(), "",
                now, processAt, null, null, null, ""));
    }

    /**
     * Rebuilds a job from storage without re-running creation rules.
     *
     * <p>This is the single sanctioned way to bypass the transition checks. It exists because
     * a repository is replaying facts that were already valid when they were written, not
     * proposing a new change.
     */
    public static Job reconstitute(JobSnapshot snapshot) {
        return new Job(snapshot);
    }

    /**
     * Returns an independent copy of the job.
     *
     * <p>A repository hands out a job the caller is free to transition, so two callers must
     * never receive the same instance. The Redis implementation gets this for free by
     * decoding a fresh aggregate per read; an in-memory implementation has to copy.
     */
    public Job copy() {
        return new Job(snapshot());
    }

    /** Captures the full state of the job, for persistence. */
    public JobSnapshot snapshot() {
        return new JobSnapshot(
                id, queue, taskType, payload.clone(), priority, state, retryPolicy,
                idempotencyKey, timeout, attemptsMade, List.copyOf(attempts), lastError,
                createdAt, processAt, completedAt, diedAt, leaseExpiry, workerId);
    }

    // ------------------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------------------

    public JobId id() {
        return id;
    }

    public QueueName queue() {
        return queue;
    }

    public TaskType taskType() {
        return taskType;
    }

    /** Returns a defensive copy of the payload. */
    public byte[] payload() {
        return payload.clone();
    }

    public Priority priority() {
        return priority;
    }

    public JobState state() {
        return state;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public IdempotencyKey idempotencyKey() {
        return idempotencyKey;
    }

    /** The per-attempt execution budget. */
    public Duration timeout() {
        return timeout;
    }

    public int attemptsMade() {
        return attemptsMade;
    }

    /** Returns an unmodifiable view of the execution trail. */
    public List<Attempt> attempts() {
        return List.copyOf(attempts);
    }

    public String lastError() {
        return lastError;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /** The instant the job becomes eligible to run. */
    public Instant processAt() {
        return processAt;
    }

    /** The success instant, or {@code null} when the job has not succeeded. */
    public Instant completedAt() {
        return completedAt;
    }

    /** The dead-letter instant, or {@code null} when the job is not dead. */
    public Instant diedAt() {
        return diedAt;
    }

    /** The instant the current worker's lease lapses, or {@code null}. */
    public Instant leaseExpiry() {
        return leaseExpiry;
    }

    /** The worker currently or most recently holding the job, or empty. */
    public String workerId() {
        return workerId;
    }

    /** Reports how many retries the job has left. */
    public int remainingRetries() {
        int max = retryPolicy.maxRetries();
        return attemptsMade > max ? 0 : max - attemptsMade;
    }

    // ------------------------------------------------------------------------------------
    // Lifecycle transitions
    // ------------------------------------------------------------------------------------

    /**
     * Moves a scheduled or retrying job into the pending set, making it eligible for
     * dequeue. The scheduler calls this once {@code processAt} has elapsed.
     */
    public void promote(Instant now) {
        guard(JobState.PENDING);
        state = JobState.PENDING;
        processAt = now;
        leaseExpiry = null;
    }

    /**
     * Marks the job active for a worker and records the lease expiry.
     *
     * <p>The lease is what makes at-least-once delivery safe: if the worker dies, the lease
     * lapses and the recovery sweep returns the job to pending. It is derived from the job
     * timeout plus a grace margin so a healthy-but-slow handler is not stolen from under
     * itself.
     */
    public void lease(String worker, Instant now, Duration grace) {
        guard(JobState.ACTIVE);
        if (worker == null || worker.isEmpty()) {
            throw AppError.invalid(JobErrorCodes.ILLEGAL_STATE_TRANSITION, "worker id is required to lease a job");
        }
        state = JobState.ACTIVE;
        workerId = worker;
        leaseExpiry = now.plus(timeout).plus(grace);
        attemptsMade++;
        attempts.add(new Attempt(attemptsMade, now, null, "", worker));
    }

    /** Marks a successful execution. */
    public void complete(Instant now) {
        guard(JobState.COMPLETED);
        state = JobState.COMPLETED;
        completedAt = now;
        leaseExpiry = null;
        lastError = "";
        closeCurrentAttempt(now, "");
    }

    /**
     * Records a failed execution and decides, by the job's own retry policy, whether the job
     * retries or moves to the dead-letter queue.
     *
     * <p>The decision lives here rather than in the processor because "is this job out of
     * retries" is an invariant of the job, and putting it in a service would let two callers
     * disagree about it.
     */
    public FailureOutcome fail(Throwable cause, Instant now) {
        if (state != JobState.ACTIVE) {
            throw AppError.invalid(JobErrorCodes.ILLEGAL_STATE_TRANSITION,
                    "only an active job can fail, current state: " + state);
        }
        String message = AppError.describe(cause);
        lastError = message;
        closeCurrentAttempt(now, message);
        leaseExpiry = null;

        if (!retryPolicy.allowsRetry(attemptsMade)) {
            state = JobState.DEAD;
            diedAt = now;
            return new FailureOutcome(false, null, attemptsMade);
        }

        Instant retryAt = now.plus(retryPolicy.nextDelay(attemptsMade));
        state = JobState.RETRYING;
        processAt = retryAt;
        return new FailureOutcome(true, retryAt, attemptsMade);
    }

    /**
     * Moves a job straight to the dead-letter queue, skipping any remaining retries. It backs
     * the operator action "stop trying this job".
     */
    public void kill(String reason, Instant now) {
        guard(JobState.DEAD);
        state = JobState.DEAD;
        diedAt = now;
        leaseExpiry = null;
        if (reason != null && !reason.isEmpty()) {
            lastError = reason;
        }
        closeCurrentAttempt(now, lastError);
    }

    /**
     * Returns a dead job to the pending set and resets its attempt counter, giving it a fresh
     * retry budget. It backs the dashboard's "retry" button.
     */
    public void requeue(Instant now) {
        if (state != JobState.DEAD) {
            throw AppError.invalid(JobErrorCodes.JOB_NOT_RETRYABLE,
                    "only a dead job can be requeued, current state: " + state);
        }
        state = JobState.PENDING;
        processAt = now;
        attemptsMade = 0;
        diedAt = null;
        leaseExpiry = null;
        workerId = "";
    }

    /**
     * Returns an active job whose lease has lapsed to the pending set. The failed attempt is
     * closed with an explicit reason, so the trail shows the job was orphaned rather than
     * silently restarted.
     */
    public void reclaimExpiredLease(Instant now) {
        if (state != JobState.ACTIVE) {
            throw AppError.invalid(JobErrorCodes.ILLEGAL_STATE_TRANSITION,
                    "only an active job can be reclaimed, current state: " + state);
        }
        closeCurrentAttempt(now, LEASE_EXPIRED_MESSAGE);
        lastError = LEASE_EXPIRED_MESSAGE;
        state = JobState.PENDING;
        processAt = now;
        leaseExpiry = null;
        workerId = "";
    }

    /** Reports whether an active job's lease has lapsed as of {@code now}. */
    public boolean isLeaseExpired(Instant now) {
        if (state != JobState.ACTIVE || leaseExpiry == null) {
            return false;
        }
        return now.isAfter(leaseExpiry);
    }

    /** Reports whether a scheduled or retrying job is ready to be promoted. */
    public boolean isDue(Instant now) {
        return processAt == null || !processAt.isAfter(now);
    }

    /** Rejects an illegal transition using the lifecycle graph owned by {@link JobState}. */
    private void guard(JobState next) {
        if (!state.canTransitionTo(next)) {
            throw AppError.invalid(JobErrorCodes.ILLEGAL_STATE_TRANSITION,
                    "cannot transition from " + state + " to " + next);
        }
    }

    /**
     * Stamps the open attempt with its outcome. A no-op when there is no open attempt, which
     * keeps {@link #kill} safe to call on a job that has never run.
     */
    private void closeCurrentAttempt(Instant finishedAt, String errorMessage) {
        if (attempts.isEmpty()) {
            return;
        }
        int last = attempts.size() - 1;
        Attempt current = attempts.get(last);
        if (current.isFinished()) {
            return;
        }
        attempts.set(last, current.finish(finishedAt, errorMessage));
    }

    @Override
    public String toString() {
        return "Job[id=" + id + ", queue=" + queue + ", taskType=" + taskType + ", state=" + state + "]";
    }

    // ------------------------------------------------------------------------------------
    // Construction parameters
    // ------------------------------------------------------------------------------------

    /**
     * The input to {@link #create}. A builder is used instead of a long positional argument
     * list because the constructor has ten meaningful inputs, several of them optional —
     * positional arguments would be unreadable and easy to transpose at the call site.
     */
    public static final class NewJobParams {
        private JobId id;
        private QueueName queue;
        private TaskType taskType;
        private byte[] payload;
        private Priority priority;
        private RetryPolicy retryPolicy;
        private IdempotencyKey idempotencyKey;
        private Duration timeout;
        private Instant processAt;
        private Instant now;

        public static NewJobParams builder() {
            return new NewJobParams();
        }

        public NewJobParams id(JobId value) {
            this.id = value;
            return this;
        }

        public NewJobParams id(String value) {
            return id(new JobId(value));
        }

        public NewJobParams queue(QueueName value) {
            this.queue = value;
            return this;
        }

        public NewJobParams taskType(TaskType value) {
            this.taskType = value;
            return this;
        }

        public NewJobParams taskType(String value) {
            return taskType(new TaskType(value));
        }

        public NewJobParams payload(byte[] value) {
            this.payload = value;
            return this;
        }

        public NewJobParams priority(Priority value) {
            this.priority = value;
            return this;
        }

        public NewJobParams retryPolicy(RetryPolicy value) {
            this.retryPolicy = value;
            return this;
        }

        public NewJobParams idempotencyKey(IdempotencyKey value) {
            this.idempotencyKey = value;
            return this;
        }

        public NewJobParams timeout(Duration value) {
            this.timeout = value;
            return this;
        }

        /**
         * When the job becomes eligible to run. {@code null} or an instant at or before
         * {@code now} means "run immediately".
         */
        public NewJobParams processAt(Instant value) {
            this.processAt = value;
            return this;
        }

        /**
         * The creation instant, supplied by the caller's clock so that the aggregate stays
         * free of ambient time.
         */
        public NewJobParams now(Instant value) {
            this.now = value;
            return this;
        }

        public Job build() {
            return Job.create(this);
        }
    }
}

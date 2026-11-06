package com.jqueue.domain.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.QueueName;

/**
 * The queueing port: the atomic operations that move a job between lifecycle sets.
 *
 * <p>Every method must be atomic with respect to concurrent workers. The Redis
 * implementation achieves that with Lua scripts; an in-memory implementation achieves it
 * with a lock. Neither detail belongs in the application layer, which is exactly why this
 * port exists.
 */
public interface JobBroker {

    /** Makes a job immediately eligible for dequeue, writing its document in the same step. */
    void enqueue(Job job);

    /** Stores a job for future execution at {@code job.processAt()}. */
    void schedule(Job job);

    /**
     * Atomically claims the highest-priority eligible job from the first queue in
     * {@code order} that has one, moves it to the active set and returns it already leased
     * to {@code workerId}.
     *
     * <p>Empty is the normal idle case, not a failure, so the worker loop can poll without
     * treating emptiness as an error condition.
     */
    Optional<Job> dequeue(List<QueueName> order, String workerId, Duration leaseFor);

    /** Moves an active job to the completed set with a retention TTL. */
    void complete(Job job, Duration retainFor);

    /** Moves an active job to the retry set, to be promoted at {@code retryAt}. */
    void retry(Job job, Instant retryAt);

    /** Moves a job to the dead-letter set. */
    void kill(Job job);

    /** Moves a dead job back to the pending set. */
    void requeue(Job job);

    /**
     * Moves scheduled and retrying jobs whose instant has arrived into the pending set, and
     * reports how many moved.
     */
    long promoteDue(QueueName queue, Instant now, long limit);

    /**
     * Returns active jobs whose lease lapsed to the pending set and reports how many were
     * reclaimed. This is the recovery path for a worker that was killed mid-job.
     */
    long reclaimExpired(QueueName queue, Instant now, long limit);

    /**
     * Pushes an active job's lease expiry further out, so a long-running handler is not
     * reclaimed while it is still making progress.
     */
    void extendLease(JobId id, QueueName queue, Instant until);
}

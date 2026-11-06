package com.jqueue.infrastructure.redis;

import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;

/**
 * Renders every Redis key used by JQueue.
 *
 * <p>Centralising key construction means the layout is documented in exactly one file, and
 * a migration to a new layout touches one type. The layout below the namespace is the same
 * as the original Go GoQueue's, so with {@code namespace: goqueue} the two can share data.
 *
 * <table>
 *   <caption>Key layout</caption>
 *   <tr><td>{ns}:job:{id}</td><td>hash</td><td>the job document</td></tr>
 *   <tr><td>{ns}:queue:{q}:pending</td><td>zset</td><td>eligible, scored by priority then age</td></tr>
 *   <tr><td>{ns}:queue:{q}:active</td><td>zset</td><td>leased, scored by lease expiry</td></tr>
 *   <tr><td>{ns}:queue:{q}:scheduled</td><td>zset</td><td>future jobs, scored by process-at</td></tr>
 *   <tr><td>{ns}:queue:{q}:retrying</td><td>zset</td><td>failed jobs, scored by next attempt</td></tr>
 *   <tr><td>{ns}:queue:{q}:completed</td><td>zset</td><td>succeeded, scored by retention deadline</td></tr>
 *   <tr><td>{ns}:queue:{q}:dead</td><td>zset</td><td>dead-letter, scored by time of death</td></tr>
 *   <tr><td>{ns}:queue:{q}:meta</td><td>hash</td><td>weight and paused flag</td></tr>
 *   <tr><td>{ns}:queues</td><td>set</td><td>the queue registry</td></tr>
 *   <tr><td>{ns}:idempotency:{key}</td><td>string</td><td>deduplication claim, with TTL</td></tr>
 * </table>
 */
public final class KeyBuilder {

    /** Prefixes every key so several deployments can share one Redis instance. */
    public static final String DEFAULT_NAMESPACE = "jqueue";

    /**
     * The score multiplier that separates priority levels inside the pending sorted set.
     *
     * <p>A pending job's score is {@code (MAX_PRIORITY - priority) * BAND + enqueuedAtMs},
     * so ZPOPMIN yields the highest priority first and, within one priority, the oldest job
     * first. The band is 10^14: wide enough that millisecond timestamps can never bleed into
     * the next band for thousands of years, and small enough that the largest score stays
     * well inside the 2^53 range where floating-point arithmetic in Lua is exact.
     */
    public static final long PRIORITY_BAND = 100_000_000_000_000L;

    private final String namespace;

    public KeyBuilder(String namespace) {
        this.namespace = namespace == null || namespace.isEmpty() ? DEFAULT_NAMESPACE : namespace;
    }

    public String namespace() {
        return namespace;
    }

    /** The hash holding one job document. */
    public String job(JobId id) {
        return namespace + ":job:" + id.value();
    }

    /** The prefix Lua scripts concatenate with a job id. */
    public String jobPrefix() {
        return namespace + ":job:";
    }

    public String pending(QueueName queue) {
        return queueKey(queue, "pending");
    }

    public String active(QueueName queue) {
        return queueKey(queue, "active");
    }

    public String scheduled(QueueName queue) {
        return queueKey(queue, "scheduled");
    }

    public String retrying(QueueName queue) {
        return queueKey(queue, "retrying");
    }

    public String completed(QueueName queue) {
        return queueKey(queue, "completed");
    }

    public String dead(QueueName queue) {
        return queueKey(queue, "dead");
    }

    /** The sorted set backing a given lifecycle state. */
    public String stateSet(QueueName queue, JobState state) {
        return switch (state) {
            case PENDING -> pending(queue);
            case ACTIVE -> active(queue);
            case SCHEDULED -> scheduled(queue);
            case RETRYING -> retrying(queue);
            case COMPLETED -> completed(queue);
            case DEAD -> dead(queue);
        };
    }

    /** The set holding every known queue name. */
    public String queueRegistry() {
        return namespace + ":queues";
    }

    /** The hash holding a queue's weight and paused flag. */
    public String queueMeta(QueueName queue) {
        return queueKey(queue, "meta");
    }

    /** The string key holding a deduplication claim. */
    public String idempotency(IdempotencyKey key) {
        return namespace + ":idempotency:" + key.value();
    }

    /** Computes the sorted-set score for a pending job. */
    public static long pendingScore(Priority priority, long enqueuedAtMs) {
        return (long) (Priority.MAX_LEVEL - priority.level()) * PRIORITY_BAND + enqueuedAtMs;
    }

    private String queueKey(QueueName queue, String suffix) {
        return namespace + ":queue:" + queue.value() + ":" + suffix;
    }
}

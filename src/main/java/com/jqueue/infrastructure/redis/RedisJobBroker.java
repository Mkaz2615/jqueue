package com.jqueue.infrastructure.redis;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.queue.Weight;
import redis.clients.jedis.UnifiedJedis;

/**
 * Implements the queueing port on Redis.
 *
 * <p>It owns exactly one thing: moving jobs between lifecycle sets atomically. It does not
 * decide <em>whether</em> a job should retry — that is the Job aggregate's decision, already
 * made by the time a method here is called.
 */
public final class RedisJobBroker implements JobBroker {

    /**
     * Added to a job's timeout when computing its lease expiry.
     *
     * <p>Without a margin, a handler that runs for exactly its timeout races the reclaim
     * sweep and can be duplicated at the finish line. Thirty seconds absorbs GC pauses and a
     * slow Redis round trip, while a genuinely dead worker's jobs still come back quickly.
     */
    static final Duration LEASE_GRACE = Duration.ofSeconds(30);

    private static final long DEFAULT_SWEEP_LIMIT = 500;

    private final UnifiedJedis redis;
    private final KeyBuilder keys;
    private final Clock clock;

    public RedisJobBroker(UnifiedJedis redis, KeyBuilder keys, Clock clock) {
        this.redis = redis;
        this.keys = keys;
        this.clock = clock;
    }

    @Override
    public void enqueue(Job job) {
        long score = KeyBuilder.pendingScore(job.priority(), JobDocumentMapper.toMillis(job.processAt()));
        insert(job, keys.pending(job.queue()), score);
    }

    @Override
    public void schedule(Job job) {
        insert(job, keys.scheduled(job.queue()), JobDocumentMapper.toMillis(job.processAt()));
    }

    /** The shared body of enqueue and schedule. */
    private void insert(Job job, String targetKey, long score) {
        List<String> args = new ArrayList<>();
        args.add(job.id().value());
        args.add(Long.toString(score));
        args.add(job.queue().value());
        args.add(Integer.toString(Weight.DEFAULT.value()));
        args.addAll(JobDocumentMapper.toFlatFields(job));

        List<String> scriptKeys = List.of(
                keys.job(job.id()), targetKey, keys.queueRegistry(), keys.queueMeta(job.queue()));

        long inserted;
        try {
            inserted = Scripts.ENQUEUE.runForLong(redis, scriptKeys, args);
        } catch (RuntimeException e) {
            throw AppError.internal("job_enqueue_failed", "failed to enqueue job", e);
        }
        if (inserted == 0) {
            throw AppError.conflict("duplicate_job_id", "a job with this id already exists");
        }
    }

    /**
     * Claims the next eligible job for a worker.
     *
     * <p>The returned job is already leased: the Lua script moved it into the active set and
     * stamped the lease before returning, and this method then replays the domain's lease
     * transition on the reconstituted aggregate so the attempt trail and retry accounting
     * stay owned by the Job.
     */
    @Override
    public Optional<Job> dequeue(List<QueueName> order, String workerId, Duration leaseFor) {
        if (order.isEmpty()) {
            return Optional.empty();
        }

        // Collapse the weighted polling order into its distinct queues. The weights have
        // already done their job upstream by deciding which queue leads this sweep; sending
        // the same key three times would only make the script re-check an empty set.
        List<QueueName> distinct = new ArrayList<>(new LinkedHashSet<>(order));

        List<String> scriptKeys = new ArrayList<>(distinct.size() * 3);
        for (QueueName queue : distinct) {
            scriptKeys.add(keys.pending(queue));
            scriptKeys.add(keys.active(queue));
            scriptKeys.add(keys.queueMeta(queue));
        }

        Instant now = clock.instant();
        Instant leaseExpiry = now.plus(leaseFor).plus(LEASE_GRACE);

        Object reply;
        try {
            reply = Scripts.DEQUEUE.run(redis, scriptKeys, List.of(
                    Integer.toString(distinct.size()),
                    workerId,
                    Long.toString(leaseExpiry.toEpochMilli()),
                    Long.toString(now.toEpochMilli()),
                    keys.jobPrefix()));
        } catch (RuntimeException e) {
            throw AppError.internal("job_dequeue_failed", "failed to dequeue job", e);
        }

        // A nil reply is the script returning false: every queue was empty. That is the
        // steady state of an idle worker, not a failure.
        if (!(reply instanceof List<?> raw) || raw.isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> hash = JobDocumentMapper.flatListToMap(raw);
        Job job = JobDocumentMapper.fromHash(hash);

        // Apply the domain transition on top of the snapshot the script returned.
        job.lease(workerId, now, LEASE_GRACE);
        return Optional.of(job);
    }

    @Override
    public void complete(Job job, Duration retainFor) {
        Instant retainUntil = clock.instant().plus(retainFor);
        transition(job, keys.active(job.queue()), keys.completed(job.queue()),
                retainUntil.toEpochMilli(), retainFor.toSeconds());
    }

    @Override
    public void retry(Job job, Instant retryAt) {
        transition(job, keys.active(job.queue()), keys.retrying(job.queue()), retryAt.toEpochMilli(), 0);
    }

    /**
     * Moves a job to the dead-letter set.
     *
     * <p>The source set is not assumed to be active, because an operator can kill a job that
     * is still scheduled or waiting on a retry.
     */
    @Override
    public void kill(Job job) {
        Instant diedAt = job.diedAt() != null ? job.diedAt() : clock.instant();
        transitionFromAny(job, keys.dead(job.queue()), diedAt.toEpochMilli());
    }

    @Override
    public void requeue(Job job) {
        long score = KeyBuilder.pendingScore(job.priority(), JobDocumentMapper.toMillis(job.processAt()));
        transition(job, keys.dead(job.queue()), keys.pending(job.queue()), score, 0);
    }

    /** Runs the shared move-and-rewrite script. */
    private void transition(Job job, String fromKey, String toKey, long score, long ttlSeconds) {
        List<String> args = new ArrayList<>();
        args.add(job.id().value());
        args.add(Long.toString(score));
        args.add(Long.toString(ttlSeconds));
        args.addAll(JobDocumentMapper.toFlatFields(job));

        try {
            Scripts.TRANSITION.runForLong(redis, List.of(keys.job(job.id()), fromKey, toKey), args);
        } catch (RuntimeException e) {
            throw AppError.internal("job_transition_failed", "failed to transition job", e);
        }
    }

    /**
     * Performs a move whose source set is unknown, by removing the job from every set of its
     * queue before inserting it into the destination.
     *
     * <p>It costs one extra round trip and is used only on the kill path, which is rare.
     * The hot paths always know where the job came from.
     */
    private void transitionFromAny(Job job, String toKey, long score) {
        QueueName queue = job.queue();
        String id = job.id().value();
        try {
            RedisBatch.atomically(redis, batch -> {
                batch.zrem(keys.pending(queue), id);
                batch.zrem(keys.active(queue), id);
                batch.zrem(keys.scheduled(queue), id);
                batch.zrem(keys.retrying(queue), id);
                batch.zrem(keys.completed(queue), id);
                batch.zrem(keys.dead(queue), id);
            });
        } catch (RuntimeException e) {
            throw AppError.internal("job_transition_failed", "failed to clear job set membership", e);
        }
        // The source set is now empty of this job, so any key works as "from".
        transition(job, keys.pending(queue), toKey, score, 0);
    }

    @Override
    public long promoteDue(QueueName queue, Instant now, long limit) {
        long effectiveLimit = limit <= 0 ? DEFAULT_SWEEP_LIMIT : limit;
        long total = 0;
        for (String sourceKey : List.of(keys.scheduled(queue), keys.retrying(queue))) {
            try {
                total += Scripts.PROMOTE.runForLong(redis,
                        List.of(sourceKey, keys.pending(queue)),
                        sweepArgs(now, effectiveLimit));
            } catch (RuntimeException e) {
                throw AppError.internal("job_promote_failed", "failed to promote due jobs", e);
            }
        }
        return total;
    }

    @Override
    public long reclaimExpired(QueueName queue, Instant now, long limit) {
        long effectiveLimit = limit <= 0 ? DEFAULT_SWEEP_LIMIT : limit;
        try {
            return Scripts.RECLAIM.runForLong(redis,
                    List.of(keys.active(queue), keys.pending(queue)),
                    sweepArgs(now, effectiveLimit));
        } catch (RuntimeException e) {
            throw AppError.internal("job_reclaim_failed", "failed to reclaim expired jobs", e);
        }
    }

    @Override
    public void extendLease(JobId id, QueueName queue, Instant until) {
        long extended;
        try {
            extended = Scripts.EXTEND_LEASE.runForLong(redis,
                    List.of(keys.active(queue), keys.job(id)),
                    List.of(id.value(), Long.toString(until.toEpochMilli())));
        } catch (RuntimeException e) {
            throw AppError.internal("lease_extend_failed", "failed to extend job lease", e);
        }
        if (extended == 0) {
            throw AppError.conflict("lease_lost",
                    "job is no longer active; its lease was reclaimed by another worker");
        }
    }

    private List<String> sweepArgs(Instant now, long limit) {
        return List.of(
                Long.toString(now.toEpochMilli()),
                Long.toString(limit),
                keys.jobPrefix(),
                Long.toString(KeyBuilder.PRIORITY_BAND),
                Integer.toString(Priority.MAX_LEVEL));
    }
}

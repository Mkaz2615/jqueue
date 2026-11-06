package com.jqueue.infrastructure.redis;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.JobFilter;
import com.jqueue.domain.persistence.JobPage;
import com.jqueue.domain.persistence.JobRepository;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;

/**
 * Implements the job persistence port on Redis.
 *
 * <p>It is read-oriented: writes on the hot path go through {@link RedisJobBroker}, which
 * needs to move set membership in the same atomic step. This type serves the queries the
 * dashboard and the API ask — look one up, list a page, delete one — which have no queueing
 * semantics at all.
 */
public final class RedisJobRepository implements JobRepository {

    /** Caps an unbounded listing so a dashboard request cannot ask for a million documents. */
    static final long DEFAULT_PAGE_SIZE = 50;
    /** The hard ceiling regardless of what a caller asks for. */
    static final long MAX_PAGE_SIZE = 500;

    /** Newest first, breaking ties on id so two refreshes render the same order. */
    static final Comparator<Job> NEWEST_FIRST = Comparator
            .comparing(Job::createdAt, Comparator.nullsLast(Comparator.reverseOrder()))
            .thenComparing(job -> job.id().value());

    private final UnifiedJedis redis;
    private final KeyBuilder keys;
    private final Clock clock;

    public RedisJobRepository(UnifiedJedis redis, KeyBuilder keys, Clock clock) {
        this.redis = redis;
        this.keys = keys;
        this.clock = clock;
    }

    /**
     * Writes the full job document without touching set membership.
     *
     * <p>This is the right call after a domain transition that does not move the job
     * between sets — recording a lease, for instance, where the broker has already placed
     * the job in the active set and only the document needs to catch up.
     */
    @Override
    public void save(Job job) {
        try {
            redis.hset(keys.job(job.id()), JobDocumentMapper.toFields(job));
        } catch (RuntimeException e) {
            throw AppError.internal("job_save_failed", "failed to save job", e);
        }
    }

    @Override
    public Job findById(JobId id) {
        Map<String, String> hash;
        try {
            hash = redis.hgetAll(keys.job(id));
        } catch (RuntimeException e) {
            throw AppError.internal("job_read_failed", "failed to read job", e);
        }
        if (hash == null || hash.isEmpty()) {
            throw AppError.notFound(JobErrorCodes.JOB_NOT_FOUND, "no job found with id " + id);
        }
        return JobDocumentMapper.fromHash(hash);
    }

    /**
     * Returns a filtered, paginated page of jobs.
     *
     * <p>The filter is applied across the lifecycle sorted sets rather than by scanning the
     * key space: SCAN over a large Redis instance is an incident waiting to happen, while a
     * sorted-set range is O(log N + page).
     */
    @Override
    public JobPage list(JobFilter filter) {
        long limit = filter.limit() <= 0 ? DEFAULT_PAGE_SIZE : Math.min(filter.limit(), MAX_PAGE_SIZE);
        long offset = Math.max(filter.offset(), 0);

        List<QueueName> queues = resolveQueues(filter.queue());
        List<JobState> states = filter.state() == null ? JobState.all() : List.of(filter.state());

        List<JobId> ids = new ArrayList<>();
        long total = 0;

        try {
            for (QueueName queue : queues) {
                for (JobState state : states) {
                    String setKey = keys.stateSet(queue, state);
                    total += redis.zcard(setKey);
                    // Over-fetch by the offset so paging still works when several sets
                    // contribute to one page.
                    for (String member : redis.zrange(setKey, 0, offset + limit - 1)) {
                        try {
                            ids.add(JobId.of(member));
                        } catch (AppError ignored) {
                            // A malformed set member is skipped rather than failing the page.
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            throw AppError.internal("job_list_failed", "failed to list jobs", e);
        }

        List<Job> jobs = loadMany(ids);

        // Task type is not indexed, so it is applied in memory over the page. A dedicated
        // index would be the answer if this ever became a hot query; for a dashboard filter
        // over a bounded page it is not worth the write amplification.
        if (filter.taskType() != null) {
            jobs = jobs.stream().filter(job -> job.taskType().equals(filter.taskType())).toList();
            total = jobs.size();
        }

        // Newest first: this is a dashboard, and the job someone is looking for is almost
        // always one that just happened.
        jobs = new ArrayList<>(jobs);
        jobs.sort(NEWEST_FIRST);

        if (offset >= jobs.size()) {
            return new JobPage(List.of(), total);
        }
        int end = (int) Math.min(offset + limit, jobs.size());
        return new JobPage(jobs.subList((int) offset, end), total);
    }

    /** Removes a job document and every set entry that referenced it. */
    @Override
    public void delete(JobId id) {
        Job job = findById(id);
        QueueName queue = job.queue();
        try {
            RedisBatch.atomically(redis, batch -> {
                batch.del(keys.job(id));
                for (JobState state : JobState.all()) {
                    batch.zrem(keys.stateSet(queue, state), id.value());
                }
                if (!job.idempotencyKey().isZero()) {
                    batch.del(keys.idempotency(job.idempotencyKey()));
                }
            });
        } catch (RuntimeException e) {
            throw AppError.internal("job_delete_failed", "failed to delete job", e);
        }
    }

    @Override
    public long purgeCompleted(QueueName queue, long limit) {
        long effectiveLimit = limit <= 0 ? MAX_PAGE_SIZE : limit;
        try {
            return Scripts.PURGE_COMPLETED.runForLong(redis,
                    List.of(keys.completed(queue)),
                    List.of(Long.toString(clock.millis()), Long.toString(effectiveLimit), keys.jobPrefix()));
        } catch (RuntimeException e) {
            throw AppError.internal("job_purge_failed", "failed to purge completed jobs", e);
        }
    }

    /** Fetches several job documents in one pipeline round trip. */
    private List<Job> loadMany(List<JobId> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        List<Response<Map<String, String>>> replies = new ArrayList<>(ids.size());
        try {
            RedisBatch.pipelined(redis, batch -> {
                for (JobId id : ids) {
                    replies.add(batch.hgetAll(keys.job(id)));
                }
            });
        } catch (RuntimeException e) {
            throw AppError.internal("job_list_failed", "failed to load jobs", e);
        }

        List<Job> jobs = new ArrayList<>(replies.size());
        for (Response<Map<String, String>> reply : replies) {
            try {
                Map<String, String> hash = reply.get();
                if (hash == null || hash.isEmpty()) {
                    // A set entry whose document has expired is stale, not fatal: skip it
                    // rather than failing a whole dashboard page over one purged job.
                    continue;
                }
                jobs.add(JobDocumentMapper.fromHash(hash));
            } catch (RuntimeException skipped) {
                // A corrupt document is skipped for the same reason.
            }
        }
        return jobs;
    }

    /** Expands an optional queue filter into the set of queues to search. */
    private List<QueueName> resolveQueues(QueueName filter) {
        if (filter != null) {
            return List.of(filter);
        }
        Set<String> names;
        try {
            names = redis.smembers(keys.queueRegistry());
        } catch (RuntimeException e) {
            throw AppError.internal("queue_list_failed", "failed to list queues", e);
        }
        List<QueueName> queues = new ArrayList<>();
        for (String name : names) {
            try {
                queues.add(QueueName.of(name));
            } catch (AppError ignored) {
                // An invalid registry entry is skipped.
            }
        }
        return queues;
    }
}

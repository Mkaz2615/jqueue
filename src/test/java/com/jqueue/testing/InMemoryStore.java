package com.jqueue.testing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.IdempotencyStore;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobFilter;
import com.jqueue.domain.persistence.JobPage;
import com.jqueue.domain.persistence.JobRepository;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueErrorCodes;
import com.jqueue.domain.queue.QueueStats;
import com.jqueue.domain.queue.Weight;

/**
 * An in-memory implementation of all four persistence ports.
 *
 * <p>Its existence is the practical payoff of depending on interfaces: the enqueuer, the
 * inspector, the processor and the worker pool are all fully testable with no Redis, no
 * Docker and no network, because none of them ever knew what a Redis was.
 *
 * <p>One type implements all of the ports because, like the Redis implementation, they share
 * state: a job's set membership and its document have to stay consistent. Consumers still
 * depend on the narrow interfaces. The {@code implements} clause is the compiler checking
 * that this fake really is substitutable for the real adapters.
 */
public final class InMemoryStore implements JobRepository, JobBroker, QueueRepository, IdempotencyStore {

    private static final Comparator<Job> NEWEST_FIRST = Comparator
            .comparing(Job::createdAt, Comparator.reverseOrder())
            .thenComparing(job -> job.id().value());

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<JobId, Job> jobs = new HashMap<>();
    private final Map<QueueName, Queue> queues = new HashMap<>();
    private final Map<IdempotencyKey, JobId> claims = new HashMap<>();

    /**
     * When set, the next mutating call throws it. Tests use it to exercise rollback paths
     * that are otherwise unreachable.
     */
    private volatile RuntimeException failNext;

    public void failNext(RuntimeException error) {
        this.failNext = error;
    }

    private void throwInjectedFailure() {
        RuntimeException error = failNext;
        if (error != null) {
            failNext = null;
            throw error;
        }
    }

    // ------------------------------------------------------------------------------------
    // JobRepository
    // ------------------------------------------------------------------------------------

    @Override
    public void save(Job job) {
        lock.writeLock().lock();
        try {
            throwInjectedFailure();
            // Store a copy so the caller keeps sole ownership of its instance. Without this
            // the map would alias the caller's aggregate and two threads could mutate the
            // same Job — an aliasing bug a real store cannot have.
            jobs.put(job.id(), job.copy());
            queues.computeIfAbsent(job.queue(), name -> Queue.create(name, Weight.DEFAULT));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Job findById(JobId id) {
        lock.readLock().lock();
        try {
            Job job = jobs.get(id);
            if (job == null) {
                throw AppError.notFound(JobErrorCodes.JOB_NOT_FOUND, "no job found with id " + id);
            }
            return job.copy();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public JobPage list(JobFilter filter) {
        lock.readLock().lock();
        try {
            List<Job> matched = new ArrayList<>();
            for (Job job : jobs.values()) {
                if (filter.queue() != null && !job.queue().equals(filter.queue())) {
                    continue;
                }
                if (filter.state() != null && job.state() != filter.state()) {
                    continue;
                }
                if (filter.taskType() != null && !job.taskType().equals(filter.taskType())) {
                    continue;
                }
                matched.add(job.copy());
            }
            matched.sort(NEWEST_FIRST);

            long total = matched.size();
            long limit = filter.limit() <= 0 ? 50 : filter.limit();
            if (filter.offset() >= total) {
                return new JobPage(List.of(), total);
            }
            int end = (int) Math.min(filter.offset() + limit, total);
            return new JobPage(matched.subList((int) filter.offset(), end), total);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(JobId id) {
        lock.writeLock().lock();
        try {
            Job job = jobs.remove(id);
            if (job == null) {
                throw AppError.notFound(JobErrorCodes.JOB_NOT_FOUND, "no job found with id " + id);
            }
            if (!job.idempotencyKey().isZero()) {
                claims.remove(job.idempotencyKey());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long purgeCompleted(QueueName queue, long limit) {
        lock.writeLock().lock();
        try {
            long removed = 0;
            Iterator<Job> it = jobs.values().iterator();
            while (it.hasNext() && (limit <= 0 || removed < limit)) {
                Job job = it.next();
                if (job.queue().equals(queue) && job.state() == JobState.COMPLETED) {
                    it.remove();
                    removed++;
                }
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------------------------
    // JobBroker
    // ------------------------------------------------------------------------------------

    @Override
    public void enqueue(Job job) {
        save(job);
    }

    @Override
    public void schedule(Job job) {
        save(job);
    }

    /**
     * Claims the highest-priority pending job from the first queue in order that has one.
     *
     * <p>The write lock plays the role the Lua script plays in Redis: it makes the claim
     * indivisible, so two concurrent workers cannot take the same job. That is what lets a
     * concurrency test written against this fake mean something about the real broker.
     */
    @Override
    public Optional<Job> dequeue(List<QueueName> order, String workerId, Duration leaseFor) {
        lock.writeLock().lock();
        try {
            throwInjectedFailure();
            for (QueueName queue : order) {
                Queue registered = queues.get(queue);
                if (registered != null && registered.isPaused()) {
                    continue;
                }
                Job best = null;
                for (Job job : jobs.values()) {
                    if (!job.queue().equals(queue) || job.state() != JobState.PENDING) {
                        continue;
                    }
                    if (best == null || beats(job, best)) {
                        best = job;
                    }
                }
                if (best != null) {
                    // Lease the stored aggregate first, so the job is already active in the
                    // store and no second worker can select it, then hand back a copy.
                    best.lease(workerId, Instant.now(), leaseFor);
                    return Optional.of(best.copy());
                }
            }
            return Optional.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Higher priority first, then older first, then by id. */
    private static boolean beats(Job candidate, Job current) {
        if (!candidate.priority().equals(current.priority())) {
            return candidate.priority().compareTo(current.priority()) > 0;
        }
        if (!candidate.processAt().equals(current.processAt())) {
            return candidate.processAt().isBefore(current.processAt());
        }
        return candidate.id().value().compareTo(current.id().value()) < 0;
    }

    @Override
    public void complete(Job job, Duration retainFor) {
        save(job);
    }

    @Override
    public void retry(Job job, Instant retryAt) {
        save(job);
    }

    @Override
    public void kill(Job job) {
        save(job);
    }

    @Override
    public void requeue(Job job) {
        save(job);
    }

    @Override
    public long promoteDue(QueueName queue, Instant now, long limit) {
        lock.writeLock().lock();
        try {
            long moved = 0;
            for (Job job : jobs.values()) {
                if (limit > 0 && moved >= limit) {
                    break;
                }
                if (!job.queue().equals(queue)) {
                    continue;
                }
                if (job.state() != JobState.SCHEDULED && job.state() != JobState.RETRYING) {
                    continue;
                }
                if (!job.isDue(now)) {
                    continue;
                }
                job.promote(now);
                moved++;
            }
            return moved;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long reclaimExpired(QueueName queue, Instant now, long limit) {
        lock.writeLock().lock();
        try {
            long reclaimed = 0;
            for (Job job : jobs.values()) {
                if (limit > 0 && reclaimed >= limit) {
                    break;
                }
                if (!job.queue().equals(queue) || !job.isLeaseExpired(now)) {
                    continue;
                }
                job.reclaimExpiredLease(now);
                reclaimed++;
            }
            return reclaimed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void extendLease(JobId id, QueueName queue, Instant until) {
        lock.readLock().lock();
        try {
            if (!jobs.containsKey(id)) {
                throw AppError.conflict("lease_lost", "job is no longer active");
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------------------------
    // QueueRepository
    // ------------------------------------------------------------------------------------

    /**
     * Records a queue, preserving an existing paused flag. Registration happens on every
     * worker start, so overwriting the flag would silently resume a paused queue.
     */
    @Override
    public void register(Queue queue) {
        lock.writeLock().lock();
        try {
            Queue existing = queues.get(queue.name());
            boolean paused = existing != null && existing.isPaused();
            queues.put(queue.name(), Queue.reconstitute(queue.name(), queue.weight(), paused));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Queue findByName(QueueName name) {
        lock.readLock().lock();
        try {
            Queue queue = queues.get(name);
            if (queue == null) {
                throw AppError.notFound(QueueErrorCodes.QUEUE_NOT_FOUND, "no queue named " + name);
            }
            return Queue.reconstitute(queue.name(), queue.weight(), queue.isPaused());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Queue> listAll() {
        lock.readLock().lock();
        try {
            return queues.values().stream()
                    .map(q -> Queue.reconstitute(q.name(), q.weight(), q.isPaused()))
                    .sorted(Comparator.comparing(Queue::name))
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setPaused(QueueName name, boolean paused) {
        lock.writeLock().lock();
        try {
            Queue queue = queues.get(name);
            if (queue == null) {
                throw AppError.notFound(QueueErrorCodes.QUEUE_NOT_FOUND, "no queue named " + name);
            }
            if (paused) {
                queue.pause();
            } else {
                queue.resume();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public QueueStats stats(QueueName name) {
        lock.readLock().lock();
        try {
            return statsFor(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<QueueStats> statsAll() {
        lock.readLock().lock();
        try {
            return queues.keySet().stream().map(this::statsFor).sorted(QueueStats.BY_NAME).toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Counts jobs by state. The caller holds the lock. */
    private QueueStats statsFor(QueueName name) {
        Queue queue = queues.get(name);
        long[] counts = new long[JobState.values().length];
        for (Job job : jobs.values()) {
            if (job.queue().equals(name)) {
                counts[job.state().ordinal()]++;
            }
        }
        return new QueueStats(
                name,
                queue != null && queue.isPaused(),
                queue == null ? Weight.DEFAULT : queue.weight(),
                counts[JobState.PENDING.ordinal()],
                counts[JobState.ACTIVE.ordinal()],
                counts[JobState.SCHEDULED.ordinal()],
                counts[JobState.RETRYING.ordinal()],
                counts[JobState.COMPLETED.ordinal()],
                counts[JobState.DEAD.ordinal()]);
    }

    // ------------------------------------------------------------------------------------
    // IdempotencyStore
    // ------------------------------------------------------------------------------------

    @Override
    public ClaimResult claim(IdempotencyKey key, JobId jobId, Duration ttl) {
        if (key.isZero()) {
            return ClaimResult.won();
        }
        lock.writeLock().lock();
        try {
            JobId existing = claims.putIfAbsent(key, jobId);
            return existing == null ? ClaimResult.won() : ClaimResult.heldBy(existing);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void release(IdempotencyKey key) {
        lock.writeLock().lock();
        try {
            claims.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

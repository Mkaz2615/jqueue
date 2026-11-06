package com.jqueue.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.jqueue.common.uid.RandomIdGenerator;
import com.jqueue.domain.job.BackoffStrategy;
import com.jqueue.domain.job.FailureOutcome;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.IdempotencyStore.ClaimResult;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueStats;
import com.jqueue.infrastructure.redis.KeyBuilder;
import com.jqueue.infrastructure.redis.RedisClientFactory;
import com.jqueue.infrastructure.redis.RedisIdempotencyStore;
import com.jqueue.infrastructure.redis.RedisJobBroker;
import com.jqueue.infrastructure.redis.RedisJobRepository;
import com.jqueue.infrastructure.redis.RedisQueueRepository;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Exercises the Redis implementation against a real server.
 *
 * <p>These tests need a Redis, so they are tagged {@code integration} and run only under
 * {@code make test-integration}. The unit suite must stay runnable with nothing installed,
 * or people stop running it; the things that genuinely cannot be faked — Lua atomicity,
 * score ordering, TTL behaviour — live here. Without a reachable Redis they are skipped.
 */
@Tag("integration")
class RedisBrokerIT {

    UnifiedJedis redis;
    String namespace;
    RedisJobBroker broker;
    RedisJobRepository jobRepository;
    RedisQueueRepository queueRepository;
    RedisIdempotencyStore idempotency;
    final QueueName queue = QueueName.DEFAULT;

    static String redisAddr() {
        String addr = System.getenv("REDIS_ADDR");
        return addr == null || addr.isEmpty() ? "127.0.0.1:6379" : addr;
    }

    /**
     * Connects and gives the test its own namespace. A per-test namespace rather than
     * FLUSHDB means the suite can run against a developer's shared Redis without
     * destroying whatever else is in it.
     */
    @BeforeEach
    void connect() {
        try {
            redis = RedisClientFactory.create(new RedisClientFactory.ClientConfig(
                    List.of(redisAddr()), "", "", 0, 32, 0,
                    Duration.ofSeconds(5), Duration.ofSeconds(3), Duration.ofSeconds(3)));
        } catch (RuntimeException e) {
            assumeTrue(false, "redis is not reachable at " + redisAddr() + ": " + e.getMessage());
        }
        namespace = "jqueue-test-" + new RandomIdGenerator().newId().substring(0, 12);
        KeyBuilder keys = new KeyBuilder(namespace);
        Clock clock = Clock.systemUTC();
        broker = new RedisJobBroker(redis, keys, clock);
        jobRepository = new RedisJobRepository(redis, keys, clock);
        queueRepository = new RedisQueueRepository(redis, keys);
        idempotency = new RedisIdempotencyStore(redis, keys);
    }

    /** Namespaces are unique per test, so this bounded scan never touches anyone else's data. */
    @AfterEach
    void cleanUp() {
        if (redis == null) {
            return;
        }
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> page = redis.scan(cursor, new ScanParams().match(namespace + ":*").count(1000));
            if (!page.getResult().isEmpty()) {
                redis.del(page.getResult().toArray(String[]::new));
            }
            cursor = page.getCursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        redis.close();
    }

    Job newJob(String id, Priority priority) {
        return Job.NewJobParams.builder()
                .id(id)
                .queue(queue)
                .taskType("email.send")
                .payload("{\"to\":\"a@b.c\"}".getBytes(StandardCharsets.UTF_8))
                .priority(priority)
                .retryPolicy(RetryPolicy.defaults())
                .timeout(Duration.ofSeconds(30))
                .now(Instant.now())
                .build();
    }

    Optional<Job> dequeue(String workerId) {
        return broker.dequeue(List.of(queue), workerId, Duration.ofMinutes(1));
    }

    @Test
    void enqueueAndDequeueRoundTrip() {
        Job original = newJob("job-roundtrip", Priority.NORMAL);
        broker.enqueue(original);

        Job dequeued = broker.dequeue(List.of(queue), "worker-1", Duration.ofSeconds(30)).orElseThrow();

        // Everything must survive the encode/decode round trip, including the payload bytes
        // and the retry policy.
        assertThat(dequeued.id()).isEqualTo(original.id());
        assertThat(dequeued.taskType()).isEqualTo(original.taskType());
        assertThat(dequeued.payload()).isEqualTo(original.payload());
        assertThat(dequeued.retryPolicy()).isEqualTo(original.retryPolicy());
        assertThat(dequeued.state()).isEqualTo(JobState.ACTIVE);
        assertThat(dequeued.workerId()).isEqualTo("worker-1");
        assertThat(dequeued.attemptsMade()).isEqualTo(1);
    }

    @Test
    void dequeueReturnsEmptyOnAnEmptyQueue() {
        // An empty queue is the steady state of an idle worker, not an error.
        assertThat(dequeue("worker-1")).isEmpty();
    }

    @Test
    void dequeueRespectsPriorityThenFifo() throws Exception {
        // Enqueued low first, so anything other than priority ordering would produce a
        // different result.
        broker.enqueue(newJob("job-low", Priority.LOW));
        Thread.sleep(2);
        broker.enqueue(newJob("job-critical", Priority.CRITICAL));
        Thread.sleep(2);
        broker.enqueue(newJob("job-normal-a", Priority.NORMAL));
        Thread.sleep(2);
        broker.enqueue(newJob("job-normal-b", Priority.NORMAL));

        List<String> order = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            order.add(dequeue("worker-1").orElseThrow().id().value());
        }

        // Highest priority first; within one priority, oldest first. This is the score
        // arithmetic in KeyBuilder verified against a real ZPOPMIN.
        assertThat(order).containsExactly("job-critical", "job-normal-a", "job-normal-b", "job-low");
    }

    @Test
    void concurrentDequeueNeverDuplicatesAJob() throws Exception {
        int jobCount = 100;
        for (int i = 0; i < jobCount; i++) {
            broker.enqueue(newJob(String.format("job-%03d", i), Priority.NORMAL));
        }

        // Twenty threads racing for a hundred jobs against a real Redis. This is the test the
        // Lua script exists for: if ZPOPMIN and the move into the active set were not one
        // atomic step, a job would be claimed twice here.
        Map<JobId, AtomicInteger> claimed = new ConcurrentHashMap<>();
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(20)) {
            List<Future<?>> workers = new ArrayList<>();
            for (int w = 0; w < 20; w++) {
                String workerId = "worker-" + w;
                workers.add(pool.submit(() -> {
                    go.await();
                    for (Optional<Job> job = dequeue(workerId); job.isPresent(); job = dequeue(workerId)) {
                        claimed.computeIfAbsent(job.get().id(), k -> new AtomicInteger()).incrementAndGet();
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : workers) {
                f.get();
            }
        }

        assertThat(claimed).as("every job claimed").hasSize(jobCount);
        claimed.forEach((id, count) -> assertThat(count.get()).as("claims of %s", id).isEqualTo(1));
    }

    @Test
    void scheduledJobIsPromotedOnceDue() throws Exception {
        Job job = Job.NewJobParams.builder()
                .id("job-scheduled")
                .queue(queue)
                .taskType("email.send")
                .retryPolicy(RetryPolicy.defaults())
                .timeout(Duration.ofSeconds(1))
                .processAt(Instant.now().plusMillis(300))
                .now(Instant.now())
                .build();
        assertThat(job.state()).isEqualTo(JobState.SCHEDULED);
        broker.schedule(job);

        // Not yet due: nothing to dequeue and nothing to promote.
        assertThat(broker.promoteDue(queue, Instant.now(), 100)).isZero();
        assertThat(dequeue("worker-1")).isEmpty();

        Thread.sleep(400);

        assertThat(broker.promoteDue(queue, Instant.now(), 100)).isEqualTo(1);
        assertThat(dequeue("worker-1").orElseThrow().id().value()).isEqualTo("job-scheduled");
    }

    @Test
    void expiredLeaseIsReclaimed() {
        broker.enqueue(newJob("job-orphan", Priority.NORMAL));

        Job leased = broker.dequeue(List.of(queue), "worker-doomed", Duration.ofSeconds(1)).orElseThrow();
        jobRepository.save(leased);

        // The worker "dies" here: no complete, no retry, nothing. Sweeping at a point past
        // the lease expiry is what a crashed pod looks like to the rest of the system.
        Instant future = Instant.now().plus(Duration.ofHours(2));
        assertThat(broker.reclaimExpired(queue, future, 100)).isEqualTo(1);

        Job stored = jobRepository.findById(new JobId("job-orphan"));
        assertThat(stored.state()).isEqualTo(JobState.PENDING);
        assertThat(stored.workerId()).isEmpty();
        assertThat(stored.lastError()).contains("lease expired");

        // And it is genuinely runnable again, not just marked pending.
        assertThat(dequeue("worker-healthy").orElseThrow().id().value()).isEqualTo("job-orphan");
    }

    /**
     * Pins a limitation inherited unchanged from the Go implementation (see the README's
     * "Known limitations"). The active-set score that the reclaim sweep reads is
     * {@code now + leaseFor + 30s}, regardless of the job's own timeout, and nothing extends
     * it while the handler runs — so a five-minute job is reclaimable after 90 seconds.
     */
    @Test
    void activeLeaseIgnoresTheJobTimeout() {
        Job longJob = Job.NewJobParams.builder()
                .id("job-long")
                .queue(queue)
                .taskType("report.generate")
                .retryPolicy(RetryPolicy.defaults())
                .timeout(Duration.ofMinutes(5))
                .now(Instant.now())
                .build();
        broker.enqueue(longJob);
        Job leased = broker.dequeue(List.of(queue), "worker-1", Duration.ofSeconds(60)).orElseThrow();
        jobRepository.save(leased);

        // The domain's own lease covers the full timeout plus grace...
        assertThat(leased.leaseExpiry()).isAfter(Instant.now().plus(Duration.ofMinutes(5)));
        // ...but the sweep reclaims the still-running job well before that.
        assertThat(broker.reclaimExpired(queue, Instant.now().plus(Duration.ofSeconds(91)), 100)).isEqualTo(1);
    }

    @Test
    void idempotencyKeyAdmitsExactlyOneClaim() throws Exception {
        IdempotencyKey key = IdempotencyKey.of("order-42-confirmation");

        // Fifty threads claiming the same key at once. SET NX is the whole mechanism; this
        // proves it is sufficient under real contention.
        int attempts = 50;
        List<JobId> winners = new CopyOnWriteArrayList<>();
        List<JobId> told = new CopyOnWriteArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(attempts)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                JobId jobId = new JobId(String.format("job-%02d", i));
                futures.add(pool.submit(() -> {
                    go.await();
                    ClaimResult result = idempotency.claim(key, jobId, Duration.ofMinutes(1));
                    if (result.claimed()) {
                        winners.add(jobId);
                    } else {
                        told.add(result.existingJobId());
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        assertThat(winners).as("exactly one claim must succeed").hasSize(1);
        assertThat(told).hasSize(attempts - 1);
        // Every loser must learn the winner's id, so each can return the original job instead
        // of failing or creating a duplicate.
        assertThat(told).allMatch(winners.getFirst()::equals);
    }

    @Test
    void releasedIdempotencyKeyCanBeClaimedAgain() {
        IdempotencyKey key = IdempotencyKey.of("retryable-key");

        assertThat(idempotency.claim(key, new JobId("job-1"), Duration.ofMinutes(1)).claimed()).isTrue();
        idempotency.release(key);
        assertThat(idempotency.claim(key, new JobId("job-2"), Duration.ofMinutes(1)).claimed()).isTrue();
    }

    @Test
    void idempotencyClaimExpiresWithItsTtl() throws Exception {
        IdempotencyKey key = IdempotencyKey.of("short-lived");

        assertThat(idempotency.claim(key, new JobId("job-1"), Duration.ofMillis(100)).claimed()).isTrue();
        assertThat(idempotency.claim(key, new JobId("job-2"), Duration.ofMillis(100)).claimed()).isFalse();
        Thread.sleep(200);
        assertThat(idempotency.claim(key, new JobId("job-3"), Duration.ofMillis(100)).claimed()).isTrue();
    }

    @Test
    void pausedQueueIsNotDrained() {
        broker.enqueue(newJob("job-paused", Priority.NORMAL));
        queueRepository.setPaused(queue, true);

        assertThat(dequeue("worker-1")).as("a paused queue must not yield work").isEmpty();

        queueRepository.setPaused(queue, false);

        assertThat(dequeue("worker-1")).as("resuming must make the job available again").isPresent();
    }

    @Test
    void registerPreservesAnExistingPausedFlag() {
        broker.enqueue(newJob("job-1", Priority.NORMAL));
        queueRepository.setPaused(queue, true);

        // Register runs on every worker start. It must not resume a queue an operator paused
        // during an incident.
        Queue paused = queueRepository.findByName(queue);
        assertThat(paused.isPaused()).isTrue();
        queueRepository.register(Queue.create(queue, paused.weight()));

        assertThat(queueRepository.findByName(queue).isPaused())
                .as("registering must not resume a paused queue").isTrue();
    }

    @Test
    void statsReflectEveryLifecycleState() {
        broker.enqueue(newJob("job-pending", Priority.NORMAL));
        broker.enqueue(newJob("job-active", Priority.NORMAL));

        // Drain both, complete one and leave the other active.
        Job first = dequeue("worker-1").orElseThrow();
        jobRepository.save(first);
        Job second = dequeue("worker-1").orElseThrow();
        second.complete(Instant.now());
        broker.complete(second, Duration.ofHours(1));

        QueueStats stats = queueRepository.stats(queue);

        assertThat(stats.active()).isEqualTo(1);
        assertThat(stats.completed()).isEqualTo(1);
        assertThat(stats.pending()).isZero();
        assertThat(stats.total()).isEqualTo(2);
    }

    @Test
    void fullFailureToDeadLetterToRequeueCycle() throws Exception {
        Job job = Job.NewJobParams.builder()
                .id("job-doomed")
                .queue(queue)
                .taskType("demo.flaky")
                .retryPolicy(RetryPolicy.of(1, Duration.ofMillis(10), Duration.ofSeconds(1), BackoffStrategy.CONSTANT))
                .timeout(Duration.ofSeconds(1))
                .now(Instant.now())
                .build();
        broker.enqueue(job);

        // Attempt 1 fails and is scheduled for retry.
        Job leased = dequeue("worker-1").orElseThrow();
        FailureOutcome outcome = leased.fail(new RuntimeException("boom"), Instant.now());
        assertThat(outcome.shouldRetry()).isTrue();
        broker.retry(leased, outcome.retryAt());

        Thread.sleep(50);
        assertThat(broker.promoteDue(queue, Instant.now(), 100)).isEqualTo(1);

        // Attempt 2 fails and exhausts the budget.
        leased = dequeue("worker-1").orElseThrow();
        outcome = leased.fail(new RuntimeException("boom again"), Instant.now());
        assertThat(outcome.shouldRetry()).isFalse();
        broker.kill(leased);

        assertThat(queueRepository.stats(queue).dead()).isEqualTo(1);

        // The operator requeues it from the dashboard, and it runs again with a fresh budget.
        Job dead = jobRepository.findById(new JobId("job-doomed"));
        dead.requeue(Instant.now());
        broker.requeue(dead);

        Job revived = dequeue("worker-2").orElseThrow();
        assertThat(revived.id().value()).isEqualTo("job-doomed");
        assertThat(revived.attemptsMade()).as("requeue must reset the attempt counter").isEqualTo(1);
        assertThat(queueRepository.stats(queue).dead()).isZero();
    }

    @Test
    void completedJobsArePurgedAfterTheirRetention() throws Exception {
        broker.enqueue(newJob("job-done", Priority.NORMAL));
        Job leased = dequeue("worker-1").orElseThrow();
        leased.complete(Instant.now());
        broker.complete(leased, Duration.ofMillis(1));

        Thread.sleep(20);
        assertThat(jobRepository.purgeCompleted(queue, 100)).isEqualTo(1);
        assertThat(queueRepository.stats(queue).completed()).isZero();
    }

    @Test
    void listSortsNewestFirstAcrossStates() throws Exception {
        for (int i = 0; i < 4; i++) {
            broker.enqueue(newJob("job-" + i, Priority.NORMAL));
            Thread.sleep(2);
        }
        jobRepository.save(dequeue("worker-1").orElseThrow());

        var page = jobRepository.list(new com.jqueue.domain.persistence.JobFilter(null, null, null, 0, 10));

        // Pending and active jobs merged into one page, newest first.
        assertThat(page.total()).isEqualTo(4);
        assertThat(page.jobs()).extracting(j -> j.id().value()).containsExactly("job-3", "job-2", "job-1", "job-0");
    }

    /**
     * Pins a limitation inherited unchanged from the Go implementation (see the README's
     * "Known limitations"). The listing reads the first {@code offset + limit} members of each
     * lifecycle set, and the pending set is ordered oldest-first, so once a set holds more
     * than one page the newest jobs fall outside the window. A created-at index would fix it;
     * if that change is made, this test should be inverted to expect "job-4" first.
     */
    @Test
    void listWindowIsTakenFromTheOldestEndOfEachSet() throws Exception {
        for (int i = 0; i < 5; i++) {
            broker.enqueue(newJob("job-" + i, Priority.NORMAL));
            Thread.sleep(2);
        }

        var page = jobRepository.list(new com.jqueue.domain.persistence.JobFilter(null, JobState.PENDING, null, 0, 3));

        assertThat(page.total()).isEqualTo(5);
        assertThat(page.jobs()).extracting(j -> j.id().value()).containsExactly("job-2", "job-1", "job-0");
    }

    @Test
    void deleteRemovesTheDocumentAndEveryIndexEntry() {
        broker.enqueue(newJob("job-gone", Priority.NORMAL));

        jobRepository.delete(new JobId("job-gone"));

        assertThat(queueRepository.stats(queue).total()).isZero();
        assertThat(dequeue("worker-1")).isEmpty();
    }
}

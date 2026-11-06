package com.jqueue.infrastructure.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.jqueue.application.processor.DefaultHandlerRegistry;
import com.jqueue.application.processor.DefaultJobProcessorService;
import com.jqueue.application.processor.ProcessorConfig;
import com.jqueue.application.services.JobHandler;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.uid.RandomIdGenerator;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.testing.InMemoryStore;

/**
 * A real pool over in-memory fakes. Nothing here is mocked except storage, so the
 * concurrency behaviour under test is the production behaviour.
 */
class WorkerPoolTest {

    static final Instant CREATED = Instant.parse("2026-01-15T10:00:00Z");

    final InMemoryStore store = new InMemoryStore();
    final DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
    DefaultJobProcessorService processor;
    WorkerPool pool;

    WorkerPool newPool(int concurrency, Duration shutdownTimeout) {
        processor = new DefaultJobProcessorService(registry, store, store, MetricsRecorder.noop(),
                Clock.systemUTC(), new ProcessorConfig(Duration.ofHours(1)));
        pool = new WorkerPool(store, store, processor, MetricsRecorder.noop(), new RandomIdGenerator(),
                new WorkerPoolConfig(concurrency, Map.of(QueueName.DEFAULT_NAME, 1),
                        Duration.ofMillis(5), shutdownTimeout, Duration.ofSeconds(10)));
        return pool;
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.stop();
        }
        if (processor != null) {
            processor.close();
        }
    }

    void seedJobs(String taskType, int n) {
        RandomIdGenerator ids = new RandomIdGenerator();
        for (int i = 0; i < n; i++) {
            store.enqueue(Job.NewJobParams.builder()
                    .id(new JobId(ids.newId()))
                    .queue(QueueName.DEFAULT)
                    .taskType(taskType)
                    .priority(Priority.NORMAL)
                    .retryPolicy(RetryPolicy.defaults())
                    .timeout(Duration.ofSeconds(5))
                    .now(CREATED)
                    .build());
        }
    }

    void register(String taskType, JobHandler handler) {
        registry.register(new TaskType(taskType), handler);
    }

    @Test
    void processesEveryQueuedJobExactlyOnce() {
        Map<JobId, AtomicInteger> seen = new ConcurrentHashMap<>();
        register("email.send", job -> seen.computeIfAbsent(job.id(), k -> new AtomicInteger()).incrementAndGet());

        int jobCount = 50;
        seedJobs("email.send", jobCount);

        newPool(8, Duration.ofSeconds(5)).start();

        await().atMost(Duration.ofSeconds(10)).until(() -> seen.size() == jobCount);
        pool.stop();

        // Eight workers competing for fifty jobs: if the claim were not atomic, some job
        // would be executed twice and this is where it would show.
        seen.forEach((id, count) -> assertThat(count.get()).as("executions of %s", id).isEqualTo(1));
    }

    @Test
    void stopWaitsForInFlightJobs() throws Exception {
        AtomicInteger started = new AtomicInteger();
        AtomicInteger finished = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);

        register("demo.slow", job -> {
            started.incrementAndGet();
            release.await();
            finished.incrementAndGet();
        });
        seedJobs("demo.slow", 3);

        newPool(3, Duration.ofSeconds(5)).start();
        await().atMost(Duration.ofSeconds(5)).until(() -> started.get() == 3);

        // Stop is called while three jobs are mid-flight. It must block until they finish
        // rather than cancelling them: that is the whole promise of "graceful shutdown
        // completes in-flight jobs".
        CompletableFuture<Void> stopped = CompletableFuture.runAsync(pool::stop);
        Thread.sleep(150);
        assertThat(stopped).as("stop returned while jobs were still running").isNotDone();

        release.countDown();

        stopped.get(5, TimeUnit.SECONDS);
        assertThat(finished.get()).isEqualTo(3);
    }

    @Test
    void stopGivesUpAfterTheShutdownTimeout() {
        CountDownLatch block = new CountDownLatch(1);
        register("demo.stuck", job -> block.await());
        seedJobs("demo.stuck", 1);

        newPool(1, Duration.ofMillis(100)).start();
        await().atMost(Duration.ofSeconds(5)).until(() -> pool.activeJobs() == 1);

        long start = System.nanoTime();
        // A stuck handler must not block shutdown forever. Stop returns normally, not with an
        // error: the job's lease will lapse and the reclaim sweep will requeue it, so this
        // is a latency event rather than data loss.
        pool.stop();

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        block.countDown();
    }

    @Test
    void rejectsASecondStart() {
        register("email.send", job -> {
        });
        newPool(1, Duration.ofSeconds(1)).start();

        // A second start would silently double concurrency and leak the first set of
        // threads, so it is refused rather than tolerated.
        assertThatThrownBy(pool::start).isInstanceOf(AppError.class).hasMessageContaining("pool_already_started");
    }

    @Test
    void skipsAPausedQueue() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        register("email.send", job -> handled.incrementAndGet());
        seedJobs("email.send", 5);
        store.setPaused(QueueName.DEFAULT, true);

        newPool(2, Duration.ofSeconds(1)).start();
        Thread.sleep(200);
        pool.stop();

        // Pausing suspends consumption, not production: the five jobs are still there.
        assertThat(handled.get()).isZero();
        assertThat(store.stats(QueueName.DEFAULT).pending()).isEqualTo(5);
    }
}

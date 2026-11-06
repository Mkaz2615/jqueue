package com.jqueue.infrastructure.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.application.services.JobProcessorService;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.logging.LogContext;
import com.jqueue.common.time.Durations;
import com.jqueue.common.uid.IdGenerator;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueErrorCodes;
import com.jqueue.domain.queue.Weight;

/**
 * Runs a fixed set of worker threads against a weighted queue order.
 *
 * <p>The pool's only responsibilities are concurrency and lifecycle: how many threads
 * exist, where they get work, and how they stop. What a job <em>means</em> belongs to the
 * processor service, and what a job <em>is</em> belongs to the aggregate. Keeping the split
 * honest is what makes graceful shutdown testable without a Redis and retry policy testable
 * without a thread.
 */
public final class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobBroker broker;
    private final QueueRepository queueRepository;
    private final JobProcessorService processor;
    private final MetricsRecorder metrics;
    private final IdGenerator ids;
    private final WorkerPoolConfig config;

    /** Guards against a double start, which would silently double concurrency. */
    private final AtomicBoolean started = new AtomicBoolean();
    /** Counts jobs currently executing, published as a gauge. */
    private final AtomicLong activeJobs = new AtomicLong();

    /** The weighted polling order, computed when the pool starts. */
    private volatile List<QueueName> dequeueOrder = List.of();
    /** Released once to tell every worker to stop dequeuing. */
    private volatile CountDownLatch stopSignal = new CountDownLatch(0);
    private final List<Thread> workers = new ArrayList<>();

    public WorkerPool(
            JobBroker broker,
            QueueRepository queueRepository,
            JobProcessorService processor,
            MetricsRecorder metrics,
            IdGenerator ids,
            WorkerPoolConfig config) {
        this.broker = broker;
        this.queueRepository = queueRepository;
        this.processor = processor;
        this.metrics = metrics;
        this.ids = ids;
        this.config = config;
    }

    /**
     * Registers the configured queues and launches the worker threads.
     *
     * <p>It returns as soon as the workers are running. {@link #stop} is what waits.
     */
    public synchronized void start() {
        if (!started.compareAndSet(false, true)) {
            throw AppError.conflict("pool_already_started", "worker pool is already running");
        }
        try {
            registerQueues();
        } catch (RuntimeException e) {
            started.set(false);
            throw e;
        }

        stopSignal = new CountDownLatch(1);
        workers.clear();
        String poolId = ids.newId().substring(0, 8);
        for (int i = 0; i < config.concurrency(); i++) {
            String workerId = "worker-" + poolId + "-" + i;
            // Daemon threads, so that a worker still busy when the shutdown budget runs out
            // cannot keep the JVM alive: its job's lease lapses and another process reclaims
            // it, exactly as if this one had crashed.
            Thread thread = Thread.ofPlatform()
                    .name("jqueue-" + workerId)
                    .daemon(true)
                    .unstarted(() -> run(workerId));
            workers.add(thread);
            thread.start();
        }

        log.atInfo()
                .addKeyValue("concurrency", config.concurrency())
                .addKeyValue("queues", queueNames())
                .addKeyValue("poll_interval", Durations.format(config.pollInterval()))
                .log("worker pool started");
    }

    /**
     * Signals every worker to finish its current job and stop dequeuing, then waits up to
     * the shutdown timeout for them.
     *
     * <p>This is the graceful part: the stop signal breaks the dequeue loop, but a job
     * already in flight runs to completion, because workers are never interrupted. A worker
     * therefore never abandons a job it has started unless the shutdown budget runs out.
     */
    public synchronized void stop() {
        if (!started.get()) {
            return;
        }
        stopSignal.countDown();

        log.atInfo()
                .addKeyValue("in_flight", activeJobs.get())
                .addKeyValue("timeout", Durations.format(config.shutdownTimeout()))
                .log("worker pool draining");

        long deadline = System.nanoTime() + config.shutdownTimeout().toNanos();
        boolean clean = true;
        for (Thread worker : workers) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !join(worker, remaining)) {
                clean = false;
                break;
            }
        }
        started.set(false);

        if (clean) {
            log.info("worker pool stopped cleanly");
        } else {
            // Not an error: the jobs still running hold leases that will lapse, and the
            // reclaim sweep will requeue them. A slow shutdown is a latency event, not data
            // loss, and reporting it as a failure would make a rolling deploy look broken.
            log.atWarn()
                    .addKeyValue("in_flight", activeJobs.get())
                    .log("worker pool shutdown timed out; in-flight jobs will be reclaimed by lease expiry");
        }
    }

    /** Reports how many jobs are executing right now. */
    public long activeJobs() {
        return activeJobs.get();
    }

    /** Returns the distinct queues this pool drains. */
    public List<QueueName> queues() {
        return List.copyOf(new LinkedHashSet<>(dequeueOrder));
    }

    public boolean isRunning() {
        return started.get();
    }

    /** One worker thread: dequeue, process, repeat until stopped. */
    private void run(String workerId) {
        try (var ignored = LogContext.with("worker_id", workerId)) {
            log.debug("worker started");
            while (!isStopping()) {
                Optional<Job> next;
                try {
                    next = broker.dequeue(dequeueOrder, workerId, config.leaseDuration());
                } catch (RuntimeException e) {
                    if (isStopping()) {
                        return;
                    }
                    log.atError().addKeyValue("error", AppError.describe(e)).log("dequeue failed");
                    if (!sleep(config.pollInterval())) {
                        return;
                    }
                    continue;
                }

                if (next.isEmpty()) {
                    if (!sleep(config.pollInterval())) {
                        return;
                    }
                    continue;
                }

                Job job = next.get();
                metrics.setActiveWorkers(activeJobs.incrementAndGet());
                try {
                    processor.process(job);
                } catch (RuntimeException e) {
                    log.atError()
                            .addKeyValue("job_id", job.id().value())
                            .addKeyValue("error", AppError.describe(e))
                            .log("job processing failed");
                } finally {
                    metrics.setActiveWorkers(activeJobs.decrementAndGet());
                }
            }
            log.debug("worker stopped");
        }
    }

    private boolean isStopping() {
        return stopSignal.getCount() == 0;
    }

    /** Waits for {@code d} or until stop is signalled. Reports false when the worker should stop. */
    private boolean sleep(Duration d) {
        try {
            return !stopSignal.await(d.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean join(Thread thread, long nanos) {
        try {
            return thread.join(Duration.ofNanos(nanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Persists the configured queues and computes the polling order.
     *
     * <p>The order is built by the domain's {@link Queue#buildDequeueOrder} so fairness is
     * defined once, with no infrastructure dependencies, and exercised by the same tests
     * whatever the broker.
     */
    private void registerQueues() {
        List<Queue> queues = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : config.queues().entrySet()) {
            QueueName name = QueueName.of(entry.getKey());
            Weight weight = Weight.of(entry.getValue() == null ? 0 : entry.getValue());
            Queue queue = Queue.create(name, weight);
            queueRepository.register(queue);
            queues.add(queue);
        }
        List<QueueName> order = Queue.buildDequeueOrder(queues);
        if (order.isEmpty()) {
            throw AppError.invalid(QueueErrorCodes.NO_QUEUES_CONFIGURED, "worker pool has no queues to drain");
        }
        dequeueOrder = List.copyOf(order);
    }

    private List<String> queueNames() {
        return queues().stream().map(QueueName::value).toList();
    }
}

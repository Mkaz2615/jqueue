package com.jqueue.infrastructure.scheduler;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jqueue.common.errors.AppError;
import com.jqueue.common.logging.LogContext;
import com.jqueue.common.time.Durations;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobRepository;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueStats;

/**
 * Runs the periodic sweeps that keep a queue honest: promoting due jobs, reclaiming lapsed
 * leases, trimming retention, and sampling queue depth for the metrics endpoint.
 *
 * <p>Each sweep has its own interval because they have genuinely different urgencies.
 * Promotion decides how punctual a scheduled job is and wants to run often; retention
 * cleanup is housekeeping and can run once a minute without anyone noticing.
 */
public final class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final JobBroker broker;
    private final JobRepository jobRepository;
    private final QueueRepository queueRepository;
    private final MetricsRecorder metrics;
    private final Clock clock;
    private final SchedulerConfig config;

    private ScheduledExecutorService executor;

    public MaintenanceScheduler(
            JobBroker broker,
            JobRepository jobRepository,
            QueueRepository queueRepository,
            MetricsRecorder metrics,
            Clock clock,
            SchedulerConfig config) {
        this.broker = broker;
        this.jobRepository = jobRepository;
        this.queueRepository = queueRepository;
        this.metrics = metrics;
        this.clock = clock;
        this.config = config;
    }

    /** One unit of periodic work, applied to a single queue. */
    @FunctionalInterface
    private interface Sweep {
        void run(QueueName queue);
    }

    /** Launches every sweep loop. */
    public synchronized void start() {
        if (executor != null) {
            throw AppError.conflict("scheduler_already_started", "scheduler is already running");
        }
        executor = Executors.newScheduledThreadPool(4,
                Thread.ofPlatform().name("jqueue-sweep-", 0).daemon(true).factory());

        spawn("promote", config.promoteInterval(), this::promoteDue);
        spawn("reclaim", config.reclaimInterval(), this::reclaimExpired);
        spawn("janitor", config.janitorInterval(), this::purgeCompleted);
        spawn("metrics", config.metricsInterval(), this::sampleQueueDepth);

        log.atInfo()
                .addKeyValue("promote_interval", Durations.format(config.promoteInterval()))
                .addKeyValue("reclaim_interval", Durations.format(config.reclaimInterval()))
                .addKeyValue("janitor_interval", Durations.format(config.janitorInterval()))
                .log("scheduler started");
    }

    /**
     * Cancels every loop and waits for the current sweeps to exit.
     *
     * <p>Sweeps are idempotent and short, so there is no drain budget beyond letting the
     * running one finish: whatever a sweep did not get to, the next process picks up.
     */
    public synchronized void stop() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executor = null;
        log.info("scheduler stopped");
    }

    public synchronized boolean isRunning() {
        return executor != null;
    }

    /** Runs a sweep against every registered queue on a fixed delay. */
    private void spawn(String name, Duration interval, Sweep sweep) {
        long nanos = interval.toNanos();
        executor.scheduleWithFixedDelay(() -> runSweep(name, sweep), nanos, nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Applies one sweep to every queue, logging rather than propagating failures: a sweep
     * that fails on one tick must not stop the loop, because the next tick is the recovery
     * mechanism. (An exception escaping a scheduled task would cancel it for good.)
     */
    private void runSweep(String name, Sweep sweep) {
        try (var ignored = LogContext.with("sweep", name)) {
            List<Queue> queues;
            try {
                queues = queueRepository.listAll();
            } catch (RuntimeException e) {
                log.atError().addKeyValue("error", AppError.describe(e)).log("sweep could not list queues");
                return;
            }
            for (Queue queue : queues) {
                try {
                    sweep.run(queue.name());
                } catch (RuntimeException e) {
                    log.atError()
                            .addKeyValue("queue", queue.name().value())
                            .addKeyValue("error", AppError.describe(e))
                            .log("sweep failed for queue");
                }
            }
        } catch (Throwable unexpected) {
            log.atError().setCause(unexpected).log("sweep crashed");
        }
    }

    private void promoteDue(QueueName queue) {
        long moved = broker.promoteDue(queue, clock.instant(), config.batchLimit());
        if (moved > 0) {
            log.atDebug().addKeyValue("queue", queue.value()).addKeyValue("count", moved).log("promoted due jobs");
        }
    }

    private void reclaimExpired(QueueName queue) {
        long reclaimed = broker.reclaimExpired(queue, clock.instant(), config.batchLimit());
        if (reclaimed > 0) {
            // A warning rather than a debug line: a steady trickle of reclaims means workers
            // are dying or handlers are outliving their timeouts, and somebody should look.
            log.atWarn()
                    .addKeyValue("queue", queue.value())
                    .addKeyValue("count", reclaimed)
                    .log("reclaimed jobs with expired leases");
        }
    }

    private void purgeCompleted(QueueName queue) {
        long removed = jobRepository.purgeCompleted(queue, config.batchLimit());
        if (removed > 0) {
            log.atDebug().addKeyValue("queue", queue.value()).addKeyValue("count", removed)
                    .log("purged expired completed jobs");
        }
    }

    /**
     * Publishes the current depth of every lifecycle state.
     *
     * <p>Depth is a gauge sampled on a timer rather than maintained incrementally, because an
     * incremental counter drifts the moment anything touches Redis from outside the
     * application — a manual fix during an incident, say — and a wrong gauge is worse than a
     * slightly stale one.
     */
    private void sampleQueueDepth(QueueName queue) {
        QueueStats stats = queueRepository.stats(queue);
        metrics.setQueueDepth(queue, JobState.PENDING, stats.pending());
        metrics.setQueueDepth(queue, JobState.ACTIVE, stats.active());
        metrics.setQueueDepth(queue, JobState.SCHEDULED, stats.scheduled());
        metrics.setQueueDepth(queue, JobState.RETRYING, stats.retrying());
        metrics.setQueueDepth(queue, JobState.COMPLETED, stats.completed());
        metrics.setQueueDepth(queue, JobState.DEAD, stats.dead());
    }
}

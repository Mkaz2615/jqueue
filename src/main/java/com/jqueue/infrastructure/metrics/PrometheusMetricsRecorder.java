package com.jqueue.infrastructure.metrics;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.domain.metrics.Outcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Implements the metrics port on Micrometer, exported in the Prometheus format.
 *
 * <p>Four metrics carry the operational story of a queue — how much work flows through,
 * how long it takes, how often it is retried, and how often it fails — and everything else
 * on the Grafana dashboard is derived from them. They are defined here and nowhere else, so
 * their names and labels cannot drift between the code that emits them and the alerts that
 * read them. The provisioned Grafana dashboard queries exactly these series, and
 * {@code PrometheusMetricsRecorderTest} pins them:
 *
 * <ul>
 *   <li>{@code jqueue_jobs_processed_total{queue,task_type,status}}</li>
 *   <li>{@code jqueue_job_processing_duration_seconds{queue,task_type}} (histogram)</li>
 *   <li>{@code jqueue_job_retries_total{queue,task_type}}</li>
 *   <li>{@code jqueue_jobs_failed_total{queue,task_type,reason}}</li>
 *   <li>{@code jqueue_queue_depth{queue,state}} and {@code jqueue_active_workers}</li>
 * </ul>
 *
 * <p>Every label has bounded cardinality by construction: queue names and task types come
 * from a registry, states from a closed enum, and failure reasons from error-code
 * constants. No label ever carries a job id.
 */
public final class PrometheusMetricsRecorder implements MetricsRecorder {

    static final String JOBS_PROCESSED = "jqueue.jobs.processed";
    static final String JOB_DURATION = "jqueue.job.processing.duration";
    static final String JOB_RETRIES = "jqueue.job.retries";
    static final String JOBS_FAILED = "jqueue.jobs.failed";
    static final String QUEUE_DEPTH = "jqueue.queue.depth";
    static final String ACTIVE_WORKERS = "jqueue.active.workers";

    /**
     * Latency buckets span 5ms to 60s because job handlers live on a very different scale
     * from HTTP requests: the usual client defaults top out at 10s and would collapse every
     * slow job into +Inf.
     */
    static final Duration[] DURATION_BUCKETS = {
        Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
        Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
        Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5),
        Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(60),
    };

    private final MeterRegistry registry;
    private final Map<String, AtomicLong> depthGauges = new ConcurrentHashMap<>();
    private final AtomicLong activeWorkers = new AtomicLong();

    public PrometheusMetricsRecorder(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder(ACTIVE_WORKERS, activeWorkers, AtomicLong::get)
                .description("Number of workers currently executing a job.")
                .register(registry);
    }

    /**
     * Metric 1 — throughput. Rate over this counter is the queue's jobs per second, and
     * splitting by status gives the success rate without a second metric.
     */
    @Override
    public void recordJobProcessed(QueueName queue, TaskType taskType, Outcome outcome) {
        Counter.builder(JOBS_PROCESSED)
                .description("Total jobs that finished execution, by terminal outcome.")
                .tag("queue", queue.value())
                .tag("task_type", taskType.value())
                .tag("status", outcome.label())
                .register(registry)
                .increment();
    }

    /** Metric 2 — latency of a single execution attempt. */
    @Override
    public void recordJobDuration(QueueName queue, TaskType taskType, Duration duration) {
        Timer.builder(JOB_DURATION)
                .description("Wall-clock duration of a single job execution attempt.")
                .tag("queue", queue.value())
                .tag("task_type", taskType.value())
                .serviceLevelObjectives(DURATION_BUCKETS)
                .register(registry)
                .record(duration);
    }

    /**
     * Metric 3 — retries. Compared against jobs processed it shows how much of the
     * throughput is rework rather than progress.
     */
    @Override
    public void recordJobRetry(QueueName queue, TaskType taskType) {
        Counter.builder(JOB_RETRIES)
                .description("Total retry attempts scheduled after a failed execution.")
                .tag("queue", queue.value())
                .tag("task_type", taskType.value())
                .register(registry)
                .increment();
    }

    /**
     * Metric 4 — failures, labelled by reason so a timeout is distinguishable from a
     * handler error or an unroutable task type.
     */
    @Override
    public void recordJobFailed(QueueName queue, TaskType taskType, String reason) {
        Counter.builder(JOBS_FAILED)
                .description("Total failed job executions, by failure reason.")
                .tag("queue", queue.value())
                .tag("task_type", taskType.value())
                .tag("reason", reason == null || reason.isEmpty() ? "unknown" : reason)
                .register(registry)
                .increment();
    }

    @Override
    public void setQueueDepth(QueueName queue, JobState state, long depth) {
        String key = queue.value() + '\u0000' + state.wire();
        depthGauges.computeIfAbsent(key, k -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder(QUEUE_DEPTH, holder, AtomicLong::get)
                    .description("Number of jobs currently in a given queue and lifecycle state.")
                    .tag("queue", queue.value())
                    .tag("state", state.wire())
                    .register(registry);
            return holder;
        }).set(depth);
    }

    @Override
    public void setActiveWorkers(long count) {
        activeWorkers.set(count);
    }
}

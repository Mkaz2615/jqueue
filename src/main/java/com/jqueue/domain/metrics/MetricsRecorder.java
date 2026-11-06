package com.jqueue.domain.metrics;

import java.time.Duration;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * The measurement port.
 *
 * <p>It lives in the domain so the worker pool and the application services can emit
 * measurements without importing a metrics library. Implementations must be safe for
 * concurrent use by every worker thread and must never throw: an observability failure may
 * not take down job processing.
 */
public interface MetricsRecorder {

    /** Increments the throughput counter. */
    void recordJobProcessed(QueueName queue, TaskType taskType, Outcome outcome);

    /** Observes handler latency. */
    void recordJobDuration(QueueName queue, TaskType taskType, Duration duration);

    /** Increments the retry counter. */
    void recordJobRetry(QueueName queue, TaskType taskType);

    /**
     * Increments the failure counter, labelled by reason so a timeout is distinguishable from
     * a handler error on the dashboard.
     */
    void recordJobFailed(QueueName queue, TaskType taskType, String reason);

    /** Publishes the current depth of one queue/state pair. */
    void setQueueDepth(QueueName queue, JobState state, long depth);

    /** Publishes how many workers are currently executing a job. */
    void setActiveWorkers(long count);

    /** A recorder that discards every measurement. */
    static MetricsRecorder noop() {
        return NopMetricsRecorder.INSTANCE;
    }
}

package com.jqueue.domain.metrics;

import java.time.Duration;

import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * Discards every measurement. It is the default binding for tests, and a convenient base
 * class for a test spy that only cares about some of the calls.
 */
public class NopMetricsRecorder implements MetricsRecorder {

    static final NopMetricsRecorder INSTANCE = new NopMetricsRecorder();

    @Override
    public void recordJobProcessed(QueueName queue, TaskType taskType, Outcome outcome) {
    }

    @Override
    public void recordJobDuration(QueueName queue, TaskType taskType, Duration duration) {
    }

    @Override
    public void recordJobRetry(QueueName queue, TaskType taskType) {
    }

    @Override
    public void recordJobFailed(QueueName queue, TaskType taskType, String reason) {
    }

    @Override
    public void setQueueDepth(QueueName queue, JobState state, long depth) {
    }

    @Override
    public void setActiveWorkers(long count) {
    }
}

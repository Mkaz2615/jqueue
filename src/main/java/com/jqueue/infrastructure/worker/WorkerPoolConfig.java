package com.jqueue.infrastructure.worker;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.jqueue.domain.job.vo.QueueName;

/**
 * Tunes the worker pool. Unset or non-positive values fall back to working defaults, so a
 * zero-valued config still runs.
 *
 * @param concurrency     how many jobs run at once
 * @param queues          queue name to weight; empty resolves to the default queue
 * @param pollInterval    how long a worker sleeps after finding every queue empty. It
 *                        trades idle Redis traffic against pickup latency.
 * @param shutdownTimeout how long stop waits for in-flight jobs. Jobs still running when it
 *                        elapses are abandoned; their leases lapse and the reclaim sweep
 *                        returns them to the queue, so no work is lost.
 * @param leaseDuration   the initial lease granted on dequeue
 */
public record WorkerPoolConfig(
        int concurrency,
        Map<String, Integer> queues,
        Duration pollInterval,
        Duration shutdownTimeout,
        Duration leaseDuration) {

    public WorkerPoolConfig {
        if (concurrency <= 0) {
            concurrency = 10;
        }
        if (queues == null || queues.isEmpty()) {
            queues = Map.of(QueueName.DEFAULT_NAME, 1);
        } else {
            queues = new LinkedHashMap<>(queues);
        }
        if (pollInterval == null || !pollInterval.isPositive()) {
            pollInterval = Duration.ofMillis(500);
        }
        if (shutdownTimeout == null || !shutdownTimeout.isPositive()) {
            shutdownTimeout = Duration.ofSeconds(30);
        }
        if (leaseDuration == null || !leaseDuration.isPositive()) {
            leaseDuration = Duration.ofSeconds(60);
        }
    }
}

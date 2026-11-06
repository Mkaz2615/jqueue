package com.jqueue.infrastructure.scheduler;

import java.time.Duration;

/**
 * Tunes the sweep intervals. Unset or non-positive values fall back to working defaults.
 *
 * @param promoteInterval how often due scheduled and retrying jobs move into pending; the
 *                        floor on scheduling precision
 * @param reclaimInterval how often lapsed leases are returned to the queue
 * @param janitorInterval how often expired completed jobs are trimmed
 * @param metricsInterval how often queue depth gauges are refreshed
 * @param batchLimit      caps how many jobs one sweep moves, so a large backlog drains over
 *                        several ticks instead of blocking Redis in one long script
 */
public record SchedulerConfig(
        Duration promoteInterval,
        Duration reclaimInterval,
        Duration janitorInterval,
        Duration metricsInterval,
        long batchLimit) {

    public SchedulerConfig {
        promoteInterval = positiveOr(promoteInterval, Duration.ofSeconds(1));
        reclaimInterval = positiveOr(reclaimInterval, Duration.ofSeconds(30));
        janitorInterval = positiveOr(janitorInterval, Duration.ofMinutes(1));
        metricsInterval = positiveOr(metricsInterval, Duration.ofSeconds(5));
        if (batchLimit <= 0) {
            batchLimit = 500;
        }
    }

    private static Duration positiveOr(Duration value, Duration fallback) {
        return value == null || !value.isPositive() ? fallback : value;
    }
}

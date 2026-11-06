package com.jqueue.application.processor;

import java.time.Duration;

/**
 * Tunes processing behaviour.
 *
 * @param completedRetention how long a succeeded job's document is kept so it remains
 *                           visible on the dashboard
 */
public record ProcessorConfig(Duration completedRetention) {

    public ProcessorConfig {
        if (completedRetention == null || !completedRetention.isPositive()) {
            completedRetention = Duration.ofHours(1);
        }
    }
}

package com.jqueue.application.enqueuer;

import java.time.Duration;

/**
 * Tunes enqueue behaviour.
 *
 * @param idempotencyTtl how long a deduplication claim is held. It should comfortably exceed
 *                       the longest retry window a client might use, or a retried request
 *                       could slip past deduplication.
 */
public record EnqueuerConfig(Duration idempotencyTtl) {

    public EnqueuerConfig {
        if (idempotencyTtl == null || !idempotencyTtl.isPositive()) {
            idempotencyTtl = Duration.ofHours(24);
        }
    }
}

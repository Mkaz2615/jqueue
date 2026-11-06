package com.jqueue.application.services;

import com.jqueue.contracts.job.EnqueueJobRequest;
import com.jqueue.contracts.job.EnqueueJobResponse;

/**
 * The write side of the job API: turns a client request into a persisted, queued job.
 *
 * <p>Service interfaces are split by <em>caller need</em> rather than by implementation. The
 * HTTP API needs enqueueing and inspection, the worker pool needs processing, and neither is
 * forced to depend on methods it never calls.
 */
public interface EnqueuerService {

    /**
     * Validates a request, applies idempotency, and places the job on its queue —
     * immediately, or at its scheduled instant.
     */
    EnqueueJobResponse enqueue(EnqueueJobRequest request);
}

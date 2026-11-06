package com.jqueue.domain.queue;

/** The stable error codes owned by the queue aggregate. */
public final class QueueErrorCodes {

    /** A queue weight is outside 1..100. */
    public static final String INVALID_QUEUE_WEIGHT = "invalid_queue_weight";
    /** No queue is registered under a name. */
    public static final String QUEUE_NOT_FOUND = "queue_not_found";
    /** Work was dispatched to a paused queue. */
    public static final String QUEUE_PAUSED = "queue_paused";
    /** A worker pool has no queues to drain. */
    public static final String NO_QUEUES_CONFIGURED = "no_queues_configured";

    private QueueErrorCodes() {
    }
}

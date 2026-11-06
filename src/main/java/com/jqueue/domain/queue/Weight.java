package com.jqueue.domain.queue;

import com.jqueue.common.errors.AppError;

/**
 * How much of a worker pool's attention a queue receives relative to its siblings. A queue
 * with weight 6 is polled six times as often as a queue with weight 1 over a long run.
 *
 * <p>Weight is queue-level fairness <em>across</em> queues; priority is job-level ordering
 * <em>within</em> a queue. Keeping them as separate concepts is what lets an operator say
 * "critical jobs go first, but the low-traffic queue never starves".
 */
public record Weight(int value) implements Comparable<Weight> {

    public static final int MIN = 1;
    public static final int MAX = 100;
    /** Applied when a queue is configured without one. */
    public static final Weight DEFAULT = new Weight(1);

    /** Validates and constructs a Weight. Zero resolves to {@link #DEFAULT}. */
    public static Weight of(int raw) {
        if (raw == 0) {
            return DEFAULT;
        }
        if (raw < MIN || raw > MAX) {
            throw AppError.invalid(QueueErrorCodes.INVALID_QUEUE_WEIGHT, "queue weight must be between 1 and 100");
        }
        return new Weight(raw);
    }

    @Override
    public int compareTo(Weight other) {
        return Integer.compare(value, other.value);
    }
}

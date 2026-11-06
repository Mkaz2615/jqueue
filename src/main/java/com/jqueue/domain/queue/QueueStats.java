package com.jqueue.domain.queue;

import java.util.Comparator;

import com.jqueue.domain.job.vo.QueueName;

/**
 * A read-model snapshot of a queue's depth per lifecycle state, built by the repository for
 * the dashboard and never mutated.
 */
public record QueueStats(
        QueueName queue,
        boolean paused,
        Weight weight,
        long pending,
        long active,
        long scheduled,
        long retrying,
        long completed,
        long dead) {

    /** Orders stats alphabetically so the dashboard renders deterministically. */
    public static final Comparator<QueueStats> BY_NAME = Comparator.comparing(QueueStats::queue);

    /** The number of jobs currently tracked for the queue. */
    public long total() {
        return pending + active + scheduled + retrying + completed + dead;
    }

    /**
     * The jobs that still have work owed to them, excluding terminal states. This is the
     * number worth alerting on.
     */
    public long backlog() {
        return pending + active + scheduled + retrying;
    }
}

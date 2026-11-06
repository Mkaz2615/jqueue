package com.jqueue.domain.persistence;

import java.util.List;

import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueStats;

/** Persists queue registration and serves the read models the dashboard renders. */
public interface QueueRepository {

    /**
     * Records that a queue exists so it shows up in listings even before it has ever held a
     * job. An existing paused flag is preserved.
     */
    void register(Queue queue);

    /** Returns one queue, or throws a NOT_FOUND {@code AppError}. */
    Queue findByName(QueueName name);

    /** Returns every registered queue. */
    List<Queue> listAll();

    /** Suspends or resumes dequeue for a queue. */
    void setPaused(QueueName name, boolean paused);

    /** Returns the per-state depth snapshot for one queue. */
    QueueStats stats(QueueName name);

    /** Returns the depth snapshot for every registered queue, sorted by name. */
    List<QueueStats> statsAll();
}

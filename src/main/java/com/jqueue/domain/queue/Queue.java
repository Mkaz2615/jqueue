package com.jqueue.domain.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.jqueue.domain.job.vo.QueueName;

/** The aggregate root describing one named queue: its weight and its paused flag. */
public final class Queue {

    private final QueueName name;
    private final Weight weight;
    private boolean paused;

    private Queue(QueueName name, Weight weight, boolean paused) {
        this.name = Objects.requireNonNull(name, "name");
        this.weight = weight == null ? Weight.DEFAULT : weight;
        this.paused = paused;
    }

    /** Builds a new, running queue. */
    public static Queue create(QueueName name, Weight weight) {
        return new Queue(name, weight, false);
    }

    /** Rebuilds a queue from storage, including its paused flag. */
    public static Queue reconstitute(QueueName name, Weight weight, boolean paused) {
        return new Queue(name, weight, paused);
    }

    public QueueName name() {
        return name;
    }

    public Weight weight() {
        return weight;
    }

    public boolean isPaused() {
        return paused;
    }

    /**
     * Suspends dequeue. Enqueue continues to work, which is deliberate: an operator pausing
     * a queue wants to stop consumption during an incident without losing the work that
     * arrives meanwhile.
     */
    public void pause() {
        paused = true;
    }

    /** Re-enables dequeue. */
    public void resume() {
        paused = false;
    }

    /**
     * Expands a set of queues into the polling order used by a worker: each queue appears as
     * many times as its weight, sorted by descending weight so heavier queues are tried first
     * on every sweep. Paused queues are left out.
     *
     * <p>This lives in the domain because queue fairness is a business rule, not a Redis
     * detail — the same ordering would apply to any broker.
     */
    public static List<QueueName> buildDequeueOrder(Collection<Queue> queues) {
        List<Queue> active = new ArrayList<>();
        for (Queue q : queues) {
            if (q != null && !q.isPaused()) {
                active.add(q);
            }
        }
        active.sort(Comparator.comparing(Queue::weight).reversed().thenComparing(Queue::name));

        List<QueueName> order = new ArrayList<>();
        for (Queue q : active) {
            for (int i = 0; i < q.weight.value(); i++) {
                order.add(q.name);
            }
        }
        return order;
    }

    @Override
    public String toString() {
        return "Queue[name=" + name + ", weight=" + weight.value() + ", paused=" + paused + "]";
    }
}

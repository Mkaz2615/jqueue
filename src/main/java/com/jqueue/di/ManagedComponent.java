package com.jqueue.di;

import org.springframework.context.SmartLifecycle;

/**
 * Adapts a background component to Spring's lifecycle, with an explicit phase.
 *
 * <p>Phases are what make shutdown correct. Spring starts lifecycle beans in ascending phase
 * order and stops them in descending order, and the embedded web server runs in a very
 * high phase. With the scheduler at {@link #SCHEDULER_PHASE} and the worker pool at
 * {@link #WORKER_POOL_PHASE}, startup goes scheduler, pool, HTTP — and shutdown goes:
 *
 * <ol>
 *   <li>HTTP listeners stop accepting (graceful), so no new work arrives;</li>
 *   <li>the worker pool drains what it already holds;</li>
 *   <li>the scheduler stops.</li>
 * </ol>
 *
 * <p>Reversing that would let the API keep enqueuing into a queue nobody is draining, and
 * let the pool claim new jobs it is about to abandon. The Redis client is closed after all
 * of them, as an ordinary bean destroy callback.
 */
final class ManagedComponent implements SmartLifecycle {

    static final int SCHEDULER_PHASE = 0;
    static final int WORKER_POOL_PHASE = 100;

    private final String name;
    private final int phase;
    private final Runnable start;
    private final Runnable stop;
    private volatile boolean running;

    ManagedComponent(String name, int phase, Runnable start, Runnable stop) {
        this.name = name;
        this.phase = phase;
        this.start = start;
        this.stop = stop;
    }

    @Override
    public void start() {
        start.run();
        running = true;
    }

    /** Blocks until the component has stopped; the component bounds its own drain time. */
    @Override
    public void stop() {
        try {
            stop.run();
        } finally {
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    @Override
    public String toString() {
        return "ManagedComponent[" + name + "]";
    }
}

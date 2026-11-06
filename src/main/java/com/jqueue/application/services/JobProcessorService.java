package com.jqueue.application.services;

import com.jqueue.domain.job.Job;

/**
 * Owns the lifecycle of a job during execution.
 *
 * <p>The worker pool deals with concurrency: how many threads run, when to stop, how to
 * drain. This service deals with what a single job's execution <em>means</em>: resolve its
 * handler, run it under a timeout, and record the outcome by asking the aggregate what
 * should happen next. Separating them is what lets the retry rules be unit-tested without
 * starting a thread, and the shutdown behaviour be tested without a Redis.
 */
public interface JobProcessorService {

    /**
     * Executes one leased job and persists its outcome.
     *
     * <p>An exception thrown from here describes a failure of the <em>processing
     * machinery</em> — an unreachable Redis, say. A handler that fails is an expected
     * outcome, recorded and retried, and does not make this method throw.
     */
    void process(Job job);
}

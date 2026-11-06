package com.jqueue.application.services;

/**
 * Executes one unit of work.
 *
 * <p>Returning normally marks the job complete. Throwing marks the attempt failed and lets
 * the job's retry policy decide what happens next:
 *
 * <ul>
 *   <li>a checked exception or an {@link com.jqueue.common.errors.AppError} is an
 *       <em>expected</em> failure, labelled by its error code (or {@code handler_error});</li>
 *   <li>any other unchecked exception is treated as a crash and labelled
 *       {@code handler_panic}, so a bug is distinguishable from a downstream outage.</li>
 * </ul>
 *
 * <p>A handler runs under the job's timeout. When the timeout elapses its thread is
 * interrupted, so a handler should let {@link InterruptedException} propagate rather than
 * swallowing it; one that ignores interruption is abandoned instead of waited for.
 */
@FunctionalInterface
public interface JobHandler {

    void handle(JobContext job) throws Exception;
}

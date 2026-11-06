package com.jqueue.application.services;

import java.util.List;

import com.jqueue.domain.job.vo.TaskType;

/**
 * Resolves a task type to the handler that executes it.
 *
 * <p>Registration is how new work types enter the system: adding one never requires editing
 * the pool, the processor, or any switch statement. That is the open/closed principle doing
 * real work.
 */
public interface HandlerRegistry {

    /**
     * Binds a handler to a task type. Registering a task type twice is an error, because the
     * second registration would silently shadow the first.
     */
    void register(TaskType taskType, JobHandler handler);

    /** Returns the handler for a task type, or throws a NOT_FOUND {@code AppError}. */
    JobHandler resolve(TaskType taskType);

    /** Lists every bound task type in sorted order, for the readiness endpoint. */
    List<TaskType> registeredTypes();
}

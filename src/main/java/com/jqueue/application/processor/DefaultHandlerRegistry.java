package com.jqueue.application.processor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jqueue.application.services.HandlerRegistry;
import com.jqueue.application.services.JobHandler;
import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.vo.TaskType;

/**
 * Maps task types to handlers.
 *
 * <p>It is the extension point of the whole system: a new kind of work is a call to
 * {@link #register}, and no existing file changes. The alternative — a switch on task type
 * inside the worker — would make every new job type an edit to the most
 * concurrency-sensitive code in the project.
 *
 * <p>Registration normally happens once at startup, but every worker thread resolves
 * against this map on every job, so it is a concurrent map regardless.
 */
public final class DefaultHandlerRegistry implements HandlerRegistry {

    private final Map<TaskType, JobHandler> handlers = new ConcurrentHashMap<>();

    /**
     * Binds a handler to a task type.
     *
     * <p>A duplicate registration is rejected rather than allowed to overwrite. Silently
     * shadowing the first handler would mean jobs quietly stop running the code someone
     * thinks they run, which is far harder to diagnose than a startup failure.
     */
    @Override
    public void register(TaskType taskType, JobHandler handler) {
        if (taskType == null || taskType.value().isEmpty()) {
            throw AppError.invalid(JobErrorCodes.INVALID_TASK_TYPE, "task type is required to register a handler");
        }
        if (handler == null) {
            throw AppError.invalid(JobErrorCodes.HANDLER_NOT_FOUND, "handler must not be null");
        }
        JobHandler previous = handlers.putIfAbsent(taskType, handler);
        if (previous != null) {
            throw AppError.conflict("duplicate_handler",
                    "a handler is already registered for task type " + taskType);
        }
    }

    @Override
    public JobHandler resolve(TaskType taskType) {
        JobHandler handler = handlers.get(taskType);
        if (handler == null) {
            throw AppError.notFound(JobErrorCodes.HANDLER_NOT_FOUND,
                    "no handler registered for task type " + taskType);
        }
        return handler;
    }

    @Override
    public List<TaskType> registeredTypes() {
        return handlers.keySet().stream().sorted().toList();
    }
}

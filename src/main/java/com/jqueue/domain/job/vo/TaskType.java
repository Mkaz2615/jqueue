package com.jqueue.domain.job.vo;

import java.util.Objects;
import java.util.regex.Pattern;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.JobErrorCodes;

/**
 * Names the unit of work a job represents. The worker pool routes a job to a handler by this
 * value, so it is part of the aggregate's identity contract rather than free-form metadata.
 */
public record TaskType(String value) implements Comparable<TaskType> {

    /** Allows dotted names such as {@code email.welcome} or {@code image:resize}. */
    private static final Pattern PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,126}$");

    public TaskType {
        Objects.requireNonNull(value, "value");
    }

    /** Validates and constructs a TaskType. */
    public static TaskType of(String raw) {
        String trimmed = raw == null ? "" : raw.strip();
        if (trimmed.isEmpty()) {
            throw AppError.invalid(JobErrorCodes.INVALID_TASK_TYPE, "task type must not be empty");
        }
        if (!PATTERN.matcher(trimmed).matches()) {
            throw AppError.invalid(JobErrorCodes.INVALID_TASK_TYPE,
                    "task type must match ^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,126}$");
        }
        return new TaskType(trimmed);
    }

    @Override
    public int compareTo(TaskType other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}

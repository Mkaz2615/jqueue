package com.jqueue.domain.job;

/**
 * The stable error codes owned by the job aggregate.
 *
 * <p>Codes are declared next to the aggregate that enforces the invariant, so there is
 * exactly one place to look when a client asks what a code means.
 */
public final class JobErrorCodes {

    /** A job identifier is empty or malformed. */
    public static final String INVALID_JOB_ID = "invalid_job_id";
    /** A task type is empty or malformed. */
    public static final String INVALID_TASK_TYPE = "invalid_task_type";
    /** A queue name is malformed. */
    public static final String INVALID_QUEUE_NAME = "invalid_queue_name";
    /** A priority falls outside the allowed range. */
    public static final String INVALID_PRIORITY = "invalid_priority";
    /** A state string has no domain equivalent. */
    public static final String INVALID_JOB_STATE = "invalid_job_state";
    /** A payload exceeds the maximum size. */
    public static final String INVALID_PAYLOAD = "invalid_payload";
    /** A retry policy is internally inconsistent. */
    public static final String INVALID_RETRY_POLICY = "invalid_retry_policy";
    /** A job timeout is out of range. */
    public static final String INVALID_TIMEOUT = "invalid_timeout";
    /** A schedule instant is missing or malformed. */
    public static final String INVALID_SCHEDULE_TIME = "invalid_schedule_time";
    /** An idempotency key is too long or contains whitespace. */
    public static final String INVALID_IDEMPOTENCY_KEY = "invalid_idempotency_key";
    /** A lifecycle transition is not allowed. */
    public static final String ILLEGAL_STATE_TRANSITION = "illegal_state_transition";
    /** No job exists for an identifier. */
    public static final String JOB_NOT_FOUND = "job_not_found";
    /** An idempotency key is already claimed. */
    public static final String DUPLICATE_JOB = "duplicate_job";
    /** A retry was requested for a job that is not in the dead-letter queue. */
    public static final String JOB_NOT_RETRYABLE = "job_not_retryable";
    /** No handler is registered for a task type. */
    public static final String HANDLER_NOT_FOUND = "handler_not_found";
    /** A handler exceeded the job timeout. */
    public static final String JOB_TIMEOUT = "job_timeout";
    /** A handler crashed with an unexpected exception. */
    public static final String HANDLER_PANIC = "handler_panic";

    private JobErrorCodes() {
    }
}

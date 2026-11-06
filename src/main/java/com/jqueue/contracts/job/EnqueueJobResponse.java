package com.jqueue.contracts.job;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Returned after a successful enqueue.
 *
 * @param deduplicated true when an idempotency key matched an existing job and this response
 *                     describes that job rather than a newly created one
 */
public record EnqueueJobResponse(
        @JsonProperty("id") String id,
        @JsonProperty("queue") String queue,
        @JsonProperty("task_type") String taskType,
        @JsonProperty("state") String state,
        @JsonProperty("priority") String priority,
        @JsonProperty("process_at") String processAt,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("deduplicated") boolean deduplicated) {
}

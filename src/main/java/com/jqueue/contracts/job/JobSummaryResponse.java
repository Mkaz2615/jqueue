package com.jqueue.contracts.job;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The trimmed representation used in listings, where the payload and the full attempt trail
 * would dominate the response size.
 */
public record JobSummaryResponse(
        @JsonProperty("id") String id,
        @JsonProperty("queue") String queue,
        @JsonProperty("task_type") String taskType,
        @JsonProperty("state") String state,
        @JsonProperty("priority") String priority,
        @JsonProperty("attempts_made") int attemptsMade,
        @JsonProperty("remaining_retries") int remainingRetries,
        @JsonProperty("last_error") @JsonInclude(JsonInclude.Include.NON_EMPTY) String lastError,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("process_at") String processAt,
        @JsonProperty("worker_id") @JsonInclude(JsonInclude.Include.NON_EMPTY) String workerId) {
}

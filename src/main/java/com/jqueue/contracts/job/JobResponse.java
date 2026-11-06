package com.jqueue.contracts.job;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

/** The full representation of a job, including its attempt trail. */
public record JobResponse(
        @JsonProperty("id") String id,
        @JsonProperty("queue") String queue,
        @JsonProperty("task_type") String taskType,
        @JsonProperty("state") String state,
        @JsonProperty("priority") String priority,
        @JsonProperty("payload") @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode payload,
        @JsonProperty("attempts_made") int attemptsMade,
        @JsonProperty("remaining_retries") int remainingRetries,
        @JsonProperty("last_error") @JsonInclude(JsonInclude.Include.NON_EMPTY) String lastError,
        @JsonProperty("timeout_seconds") long timeoutSeconds,
        @JsonProperty("idempotency_key") @JsonInclude(JsonInclude.Include.NON_EMPTY) String idempotencyKey,
        @JsonProperty("retry_policy") RetryPolicyResponse retryPolicy,
        @JsonProperty("attempts") List<AttemptResponse> attempts,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("process_at") String processAt,
        @JsonProperty("completed_at") @JsonInclude(JsonInclude.Include.NON_EMPTY) String completedAt,
        @JsonProperty("died_at") @JsonInclude(JsonInclude.Include.NON_EMPTY) String diedAt,
        @JsonProperty("lease_expiry") @JsonInclude(JsonInclude.Include.NON_EMPTY) String leaseExpiry,
        @JsonProperty("worker_id") @JsonInclude(JsonInclude.Include.NON_EMPTY) String workerId) {

    /** One execution record in a job's history. */
    public record AttemptResponse(
            @JsonProperty("number") int number,
            @JsonProperty("started_at") String startedAt,
            @JsonProperty("finished_at") @JsonInclude(JsonInclude.Include.NON_EMPTY) String finishedAt,
            @JsonProperty("duration_ms") long durationMs,
            @JsonProperty("error") @JsonInclude(JsonInclude.Include.NON_EMPTY) String error,
            @JsonProperty("worker_id") @JsonInclude(JsonInclude.Include.NON_EMPTY) String workerId) {
    }

    /** A job's failure policy. */
    public record RetryPolicyResponse(
            @JsonProperty("max_retries") int maxRetries,
            @JsonProperty("retry_base_delay_seconds") long retryBaseDelaySeconds,
            @JsonProperty("retry_max_delay_seconds") long retryMaxDelaySeconds,
            @JsonProperty("backoff_strategy") String backoffStrategy) {
    }
}

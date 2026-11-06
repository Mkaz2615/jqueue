package com.jqueue.contracts.job;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The request body for creating a job.
 *
 * <p>Contracts are deliberately separate from aggregates. An aggregate is shaped by the
 * invariants it must protect; a contract is shaped by what a client needs to send and is
 * allowed to see. Letting one type do both jobs means every domain refactor becomes a
 * breaking API change.
 *
 * <p>The constraints here are shape checks done at the HTTP boundary. The domain value
 * objects still validate every field, so a request built in code gets the same rules.
 *
 * @param taskType              routes the job to a registered handler; required
 * @param queue                 the target queue; defaults to {@code default}
 * @param payload               the handler's input, passed through opaquely
 * @param priority              one of low, normal, high, critical
 * @param delaySeconds          schedules the job that many seconds out; exclusive with runAt
 * @param runAt                 schedules the job at an RFC 3339 instant; exclusive with delaySeconds
 * @param maxRetries            overrides the default retry budget
 * @param retryBaseDelaySeconds the unit delay fed to the backoff strategy
 * @param retryMaxDelaySeconds  caps any computed backoff
 * @param backoffStrategy       one of exponential, linear, constant
 * @param timeoutSeconds        the per-attempt execution budget
 * @param idempotencyKey        while held, a repeat request returns the original job
 */
public record EnqueueJobRequest(
        @JsonProperty("task_type")
        @NotEmpty(message = "is required")
        String taskType,

        @JsonProperty("queue")
        String queue,

        @JsonProperty("payload")
        JsonNode payload,

        @JsonProperty("priority")
        @Pattern(regexp = "^(|low|normal|high|critical)$", message = "must be one of low, normal, high, critical")
        String priority,

        @JsonProperty("delay_seconds")
        @Min(value = 0, message = "must be at least 0")
        Long delaySeconds,

        @JsonProperty("run_at")
        String runAt,

        @JsonProperty("max_retries")
        @Min(value = 0, message = "must be at least 0")
        @Max(value = 100, message = "must be at most 100")
        Integer maxRetries,

        @JsonProperty("retry_base_delay_seconds")
        @Min(value = 0, message = "must be at least 0")
        @Max(value = 3600, message = "must be at most 3600")
        Long retryBaseDelaySeconds,

        @JsonProperty("retry_max_delay_seconds")
        @Min(value = 0, message = "must be at least 0")
        @Max(value = 86400, message = "must be at most 86400")
        Long retryMaxDelaySeconds,

        @JsonProperty("backoff_strategy")
        @Pattern(regexp = "^(|exponential|linear|constant)$", message = "must be one of exponential, linear, constant")
        String backoffStrategy,

        @JsonProperty("timeout_seconds")
        @Min(value = 1, message = "must be at least 1")
        @Max(value = 86400, message = "must be at most 86400")
        Long timeoutSeconds,

        @JsonProperty("idempotency_key")
        @Size(max = 128, message = "must be at most 128 characters")
        String idempotencyKey) {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** The payload as the bytes a handler will receive; empty when none was sent. */
    public byte[] payloadBytes() {
        if (payload == null || payload.isNull() || payload.isMissingNode()) {
            return new byte[0];
        }
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A fluent builder, mostly for tests and for producers calling the service in-process. */
    public static final class Builder {
        private String taskType;
        private String queue;
        private JsonNode payload;
        private String priority;
        private Long delaySeconds;
        private String runAt;
        private Integer maxRetries;
        private Long retryBaseDelaySeconds;
        private Long retryMaxDelaySeconds;
        private String backoffStrategy;
        private Long timeoutSeconds;
        private String idempotencyKey;

        public Builder taskType(String v) {
            this.taskType = v;
            return this;
        }

        public Builder queue(String v) {
            this.queue = v;
            return this;
        }

        /** Sets the payload from a JSON document. */
        public Builder payloadJson(String json) {
            this.payload = JSON.readTree(json);
            return this;
        }

        public Builder payload(JsonNode v) {
            this.payload = v;
            return this;
        }

        public Builder priority(String v) {
            this.priority = v;
            return this;
        }

        public Builder delaySeconds(Long v) {
            this.delaySeconds = v;
            return this;
        }

        public Builder runAt(String v) {
            this.runAt = v;
            return this;
        }

        public Builder maxRetries(Integer v) {
            this.maxRetries = v;
            return this;
        }

        public Builder retryBaseDelaySeconds(Long v) {
            this.retryBaseDelaySeconds = v;
            return this;
        }

        public Builder retryMaxDelaySeconds(Long v) {
            this.retryMaxDelaySeconds = v;
            return this;
        }

        public Builder backoffStrategy(String v) {
            this.backoffStrategy = v;
            return this;
        }

        public Builder timeoutSeconds(Long v) {
            this.timeoutSeconds = v;
            return this;
        }

        public Builder idempotencyKey(String v) {
            this.idempotencyKey = v;
            return this;
        }

        public EnqueueJobRequest build() {
            return new EnqueueJobRequest(taskType, queue, payload, priority, delaySeconds, runAt,
                    maxRetries, retryBaseDelaySeconds, retryMaxDelaySeconds, backoffStrategy,
                    timeoutSeconds, idempotencyKey);
        }
    }
}

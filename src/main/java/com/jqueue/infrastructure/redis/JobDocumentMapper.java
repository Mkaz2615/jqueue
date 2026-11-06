package com.jqueue.infrastructure.redis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.Attempt;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.JobErrorCodes;
import com.jqueue.domain.job.JobSnapshot;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Converts a {@link Job} to and from the Redis hash that stores it.
 *
 * <p>Field names are snake_case and stable: changing one is a storage migration, so they
 * are declared as constants rather than written inline where a typo would silently produce
 * a second field. The encoding is the same as the Go implementation's.
 */
public final class JobDocumentMapper {

    static final String F_ID = "id";
    static final String F_QUEUE = "queue";
    static final String F_TASK_TYPE = "task_type";
    static final String F_PAYLOAD = "payload";
    static final String F_PRIORITY = "priority";
    static final String F_STATE = "state";
    static final String F_MAX_RETRIES = "max_retries";
    static final String F_BASE_DELAY_MS = "base_delay_ms";
    static final String F_MAX_DELAY_MS = "max_delay_ms";
    static final String F_BACKOFF = "backoff";
    static final String F_IDEMPOTENCY_KEY = "idempotency_key";
    static final String F_TIMEOUT_MS = "timeout_ms";
    static final String F_ATTEMPTS_MADE = "attempts_made";
    static final String F_ATTEMPTS = "attempts";
    static final String F_LAST_ERROR = "last_error";
    static final String F_CREATED_AT_MS = "created_at_ms";
    static final String F_PROCESS_AT_MS = "process_at_ms";
    static final String F_COMPLETED_AT_MS = "completed_at_ms";
    static final String F_DIED_AT_MS = "died_at_ms";
    static final String F_LEASE_EXPIRY_MS = "lease_expiry_ms";
    static final String F_WORKER_ID = "worker_id";

    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();
    private static final TypeReference<List<AttemptModel>> ATTEMPTS_TYPE = new TypeReference<>() {
    };

    private JobDocumentMapper() {
    }

    /**
     * The persisted shape of one execution record.
     *
     * <p>The attempt trail is stored as a single JSON field rather than as separate hash
     * fields, because it is read and written as a unit and a JSON array keeps the hash's
     * field count fixed no matter how many times a job is retried.
     */
    record AttemptModel(
            @JsonProperty("number") int number,
            @JsonProperty("started_at_ms") long startedAtMs,
            @JsonProperty("finished_at_ms") long finishedAtMs,
            @JsonProperty("error") @JsonInclude(JsonInclude.Include.NON_EMPTY) String error,
            @JsonProperty("worker_id") @JsonInclude(JsonInclude.Include.NON_EMPTY) String workerId) {
    }

    /**
     * Flattens a job into the ordered field/value pairs written with HSET.
     *
     * <p>The payload is base64 encoded. Redis strings are binary safe, but the job document
     * is also read by the dashboard and by redis-cli during an incident, and a raw binary
     * payload makes both unreadable; base64 keeps the document inspectable at the cost of a
     * third more bytes.
     */
    public static Map<String, String> toFields(Job job) {
        List<AttemptModel> attempts = new ArrayList<>();
        for (Attempt a : job.attempts()) {
            attempts.add(new AttemptModel(
                    a.number(), toMillis(a.startedAt()), toMillis(a.finishedAt()), a.error(), a.workerId()));
        }
        String attemptsJson;
        try {
            attemptsJson = JSON.writeValueAsString(attempts);
        } catch (JacksonException e) {
            throw AppError.internal("job_encode_failed", "failed to encode job attempts", e);
        }

        RetryPolicy policy = job.retryPolicy();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(F_ID, job.id().value());
        fields.put(F_QUEUE, job.queue().value());
        fields.put(F_TASK_TYPE, job.taskType().value());
        fields.put(F_PAYLOAD, Base64.getEncoder().encodeToString(job.payload()));
        fields.put(F_PRIORITY, Integer.toString(job.priority().level()));
        fields.put(F_STATE, job.state().wire());
        fields.put(F_MAX_RETRIES, Integer.toString(policy.maxRetries()));
        fields.put(F_BASE_DELAY_MS, Long.toString(policy.baseDelay().toMillis()));
        fields.put(F_MAX_DELAY_MS, Long.toString(policy.maxDelay().toMillis()));
        fields.put(F_BACKOFF, policy.strategyName());
        fields.put(F_IDEMPOTENCY_KEY, job.idempotencyKey().value());
        fields.put(F_TIMEOUT_MS, Long.toString(job.timeout().toMillis()));
        fields.put(F_ATTEMPTS_MADE, Integer.toString(job.attemptsMade()));
        fields.put(F_ATTEMPTS, attemptsJson);
        fields.put(F_LAST_ERROR, job.lastError());
        fields.put(F_CREATED_AT_MS, Long.toString(toMillis(job.createdAt())));
        fields.put(F_PROCESS_AT_MS, Long.toString(toMillis(job.processAt())));
        fields.put(F_COMPLETED_AT_MS, Long.toString(toMillis(job.completedAt())));
        fields.put(F_DIED_AT_MS, Long.toString(toMillis(job.diedAt())));
        fields.put(F_LEASE_EXPIRY_MS, Long.toString(toMillis(job.leaseExpiry())));
        fields.put(F_WORKER_ID, job.workerId());
        return fields;
    }

    /** Renders the fields as the flat {@code [field, value, ...]} list a Lua script unpacks. */
    public static List<String> toFlatFields(Job job) {
        Map<String, String> fields = toFields(job);
        List<String> flat = new ArrayList<>(fields.size() * 2);
        fields.forEach((k, v) -> {
            flat.add(k);
            flat.add(v);
        });
        return flat;
    }

    /**
     * Rebuilds a job from a Redis hash.
     *
     * <p>Decoding is lenient about missing optional fields and strict about the ones that
     * define identity: a document without an id or a task type is corrupt, and silently
     * substituting a default would hand the worker pool a job it cannot route.
     */
    public static Job fromHash(Map<String, String> hash) {
        if (hash == null || hash.isEmpty()) {
            throw AppError.notFound(JobErrorCodes.JOB_NOT_FOUND, "job document is empty");
        }

        JobId id = JobId.of(hash.get(F_ID));
        QueueName queue = QueueName.of(hash.get(F_QUEUE));
        TaskType taskType = TaskType.of(hash.get(F_TASK_TYPE));
        JobState state = JobState.fromWire(hash.get(F_STATE));
        Priority priority = Priority.of((int) parseLong(hash.get(F_PRIORITY), Priority.NORMAL.level()));
        IdempotencyKey idempotencyKey = IdempotencyKey.of(hash.get(F_IDEMPOTENCY_KEY));

        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(hash.getOrDefault(F_PAYLOAD, ""));
        } catch (IllegalArgumentException e) {
            throw AppError.internal("job_decode_failed", "failed to decode job payload", e);
        }

        RetryPolicy policy = RetryPolicy.of(
                (int) parseLong(hash.get(F_MAX_RETRIES), RetryPolicy.DEFAULT_MAX_RETRIES),
                Duration.ofMillis(parseLong(hash.get(F_BASE_DELAY_MS), RetryPolicy.DEFAULT_BASE_DELAY.toMillis())),
                Duration.ofMillis(parseLong(hash.get(F_MAX_DELAY_MS), RetryPolicy.DEFAULT_MAX_DELAY.toMillis())),
                hash.get(F_BACKOFF));

        List<Attempt> attempts = new ArrayList<>();
        String rawAttempts = hash.get(F_ATTEMPTS);
        if (rawAttempts != null && !rawAttempts.isEmpty()) {
            List<AttemptModel> models;
            try {
                models = JSON.readValue(rawAttempts, ATTEMPTS_TYPE);
            } catch (JacksonException e) {
                throw AppError.internal("job_decode_failed", "failed to decode job attempts", e);
            }
            if (models != null) {
                for (AttemptModel m : models) {
                    attempts.add(new Attempt(m.number(), fromMillis(m.startedAtMs()), fromMillis(m.finishedAtMs()),
                            m.error(), m.workerId()));
                }
            }
        }

        return Job.reconstitute(new JobSnapshot(
                id,
                queue,
                taskType,
                payload,
                priority,
                state,
                policy,
                idempotencyKey,
                Duration.ofMillis(parseLong(hash.get(F_TIMEOUT_MS), Job.DEFAULT_TIMEOUT.toMillis())),
                (int) parseLong(hash.get(F_ATTEMPTS_MADE), 0),
                attempts,
                hash.getOrDefault(F_LAST_ERROR, ""),
                fromMillis(parseLong(hash.get(F_CREATED_AT_MS), 0)),
                fromMillis(parseLong(hash.get(F_PROCESS_AT_MS), 0)),
                fromMillis(parseLong(hash.get(F_COMPLETED_AT_MS), 0)),
                fromMillis(parseLong(hash.get(F_DIED_AT_MS), 0)),
                fromMillis(parseLong(hash.get(F_LEASE_EXPIRY_MS), 0)),
                hash.getOrDefault(F_WORKER_ID, "")));
    }

    /**
     * Converts the flat {@code [field, value, ...]} list a Lua HGETALL returns into a map. An
     * odd-length input means the script and this decoder disagree, which is a bug rather
     * than a data condition.
     */
    static Map<String, String> flatListToMap(List<?> raw) {
        if (raw.size() % 2 != 0) {
            throw AppError.internal("job_decode_failed", "malformed job hash: odd number of elements", null);
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < raw.size(); i += 2) {
            Object key = raw.get(i);
            Object value = raw.get(i + 1);
            if (key instanceof String k) {
                out.put(k, value instanceof String v ? v : "");
            }
        }
        return out;
    }

    /** Renders an instant as unix milliseconds, mapping {@code null} to 0. */
    static long toMillis(Instant t) {
        return t == null ? 0 : t.toEpochMilli();
    }

    /** Parses unix milliseconds, mapping 0 back to {@code null}. */
    static Instant fromMillis(long ms) {
        return ms == 0 ? null : Instant.ofEpochMilli(ms);
    }

    /** Parses a decimal integer, falling back to a default. */
    static long parseLong(String raw, long fallback) {
        if (raw == null || raw.isEmpty()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}

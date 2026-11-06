package com.jqueue.hydrator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.jqueue.contracts.job.EnqueueJobResponse;
import com.jqueue.contracts.job.JobResponse;
import com.jqueue.contracts.job.JobSummaryResponse;
import com.jqueue.contracts.queue.ListQueueStatsResponse;
import com.jqueue.contracts.queue.QueueStatsResponse;
import com.jqueue.domain.job.Attempt;
import com.jqueue.domain.job.Job;
import com.jqueue.domain.job.RetryPolicy;
import com.jqueue.domain.queue.QueueStats;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.StringNode;

/**
 * Maps domain aggregates onto API contracts.
 *
 * <p>The mapping lives in its own package so neither side has to know about the other: the
 * aggregate stays free of JSON annotations, and the contract stays free of value-object
 * types. It is one-directional by design — inbound requests are translated inside the
 * application services, where the validation errors they can produce belong.
 */
public final class JobHydrator {

    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    private static final DateTimeFormatter SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    private JobHydrator() {
    }

    /** Maps a job onto its full API representation. */
    public static JobResponse toJobResponse(Job job) {
        RetryPolicy policy = job.retryPolicy();
        return new JobResponse(
                job.id().value(),
                job.queue().value(),
                job.taskType().value(),
                job.state().wire(),
                job.priority().toString(),
                toPayloadNode(job.payload()),
                job.attemptsMade(),
                job.remainingRetries(),
                job.lastError(),
                job.timeout().toSeconds(),
                job.idempotencyKey().value(),
                new JobResponse.RetryPolicyResponse(
                        policy.maxRetries(),
                        policy.baseDelay().toSeconds(),
                        policy.maxDelay().toSeconds(),
                        policy.strategyName()),
                toAttempts(job.attempts()),
                formatTime(job.createdAt()),
                formatTime(job.processAt()),
                formatTime(job.completedAt()),
                formatTime(job.diedAt()),
                formatTime(job.leaseExpiry()),
                job.workerId());
    }

    /** Maps a job onto its listing representation. */
    public static JobSummaryResponse toSummary(Job job) {
        return new JobSummaryResponse(
                job.id().value(),
                job.queue().value(),
                job.taskType().value(),
                job.state().wire(),
                job.priority().toString(),
                job.attemptsMade(),
                job.remainingRetries(),
                job.lastError(),
                formatTime(job.createdAt()),
                formatTime(job.processAt()),
                job.workerId());
    }

    /** Maps a page of jobs. */
    public static List<JobSummaryResponse> toSummaries(List<Job> jobs) {
        return jobs.stream().map(JobHydrator::toSummary).toList();
    }

    /** Maps a freshly created (or deduplicated) job onto its enqueue acknowledgement. */
    public static EnqueueJobResponse toEnqueueResponse(Job job, boolean deduplicated) {
        return new EnqueueJobResponse(
                job.id().value(),
                job.queue().value(),
                job.taskType().value(),
                job.state().wire(),
                job.priority().toString(),
                formatTime(job.processAt()),
                formatTime(job.createdAt()),
                deduplicated);
    }

    /** Maps a queue stats read model onto its API representation. */
    public static QueueStatsResponse toQueueStats(QueueStats s) {
        return new QueueStatsResponse(
                s.queue().value(), s.paused(), s.weight().value(),
                s.pending(), s.active(), s.scheduled(), s.retrying(), s.completed(), s.dead(),
                s.backlog(), s.total());
    }

    /** Maps every queue and rolls up the totals. */
    public static ListQueueStatsResponse toListQueueStats(List<QueueStats> stats) {
        long pending = 0;
        long active = 0;
        long scheduled = 0;
        long retrying = 0;
        long completed = 0;
        long dead = 0;
        for (QueueStats s : stats) {
            pending += s.pending();
            active += s.active();
            scheduled += s.scheduled();
            retrying += s.retrying();
            completed += s.completed();
            dead += s.dead();
        }
        long backlog = pending + active + scheduled + retrying;
        QueueStatsResponse totals = new QueueStatsResponse(
                "all", false, 0, pending, active, scheduled, retrying, completed, dead,
                backlog, backlog + completed + dead);
        return new ListQueueStatsResponse(stats.stream().map(JobHydrator::toQueueStats).toList(), totals);
    }

    private static List<JobResponse.AttemptResponse> toAttempts(List<Attempt> attempts) {
        return attempts.stream()
                .map(a -> new JobResponse.AttemptResponse(
                        a.number(),
                        formatTime(a.startedAt()),
                        formatTime(a.finishedAt()),
                        a.duration().toMillis(),
                        a.error(),
                        a.workerId()))
                .toList();
    }

    /**
     * Passes a JSON payload through untouched and wraps a non-JSON one as a JSON string, so
     * the response is always valid JSON regardless of what a producer enqueued.
     */
    static JsonNode toPayloadNode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            return JSON.readTree(payload);
        } catch (JacksonException notJson) {
            return StringNode.valueOf(new String(payload, StandardCharsets.UTF_8));
        }
    }

    /**
     * Renders an instant as RFC 3339 with the shortest exact fraction, mapping {@code null}
     * to an empty string so clients can tell "not set" from "set to the epoch".
     */
    public static String formatTime(Instant t) {
        if (t == null) {
            return "";
        }
        String base = SECONDS.format(t);
        int nanos = t.getNano();
        if (nanos == 0) {
            return base + "Z";
        }
        String fraction = String.format("%09d", nanos);
        int end = fraction.length();
        while (end > 0 && fraction.charAt(end - 1) == '0') {
            end--;
        }
        return base + "." + fraction.substring(0, end) + "Z";
    }
}

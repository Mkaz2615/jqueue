package com.jqueue.domain.job;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.job.vo.JobState;
import com.jqueue.domain.job.vo.Priority;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.job.vo.TaskType;

/**
 * The full persisted state of a {@link Job}.
 *
 * <p>Repositories read a snapshot to write a job and build one to rebuild it with
 * {@link Job#reconstitute}. Optional instants are {@code null} when unset.
 */
public record JobSnapshot(
        JobId id,
        QueueName queue,
        TaskType taskType,
        byte[] payload,
        Priority priority,
        JobState state,
        RetryPolicy retryPolicy,
        IdempotencyKey idempotencyKey,
        Duration timeout,
        int attemptsMade,
        List<Attempt> attempts,
        String lastError,
        Instant createdAt,
        Instant processAt,
        Instant completedAt,
        Instant diedAt,
        Instant leaseExpiry,
        String workerId) {
}

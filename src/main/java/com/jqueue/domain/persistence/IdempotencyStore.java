package com.jqueue.domain.persistence;

import java.time.Duration;

import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;

/**
 * Guarantees that a given idempotency key produces at most one job while the key is held.
 *
 * <p>It is a port of its own rather than a method on {@link JobRepository} because
 * deduplication is an independent concern with its own lifetime: the claim expires on a TTL
 * while the job it protected may live on in the completed set.
 */
public interface IdempotencyStore {

    /**
     * Atomically reserves a key for a job.
     *
     * <p>Returns a claimed result when the key was free and is now held by {@code jobId}.
     * Returns an unclaimed result carrying the existing job id when the key was already
     * held, so the caller can return the original job instead of creating a duplicate.
     */
    ClaimResult claim(IdempotencyKey key, JobId jobId, Duration ttl);

    /**
     * Drops a claim, allowing the key to be reused immediately. The enqueue path calls it to
     * roll back when job persistence fails after the claim succeeded.
     */
    void release(IdempotencyKey key);

    /**
     * The outcome of a claim.
     *
     * @param claimed       true when this caller now holds the key
     * @param existingJobId the job holding the key when {@code claimed} is false; may be
     *                      {@code null} when the holder could not be determined
     */
    record ClaimResult(boolean claimed, JobId existingJobId) {

        public static ClaimResult won() {
            return new ClaimResult(true, null);
        }

        public static ClaimResult heldBy(JobId existing) {
            return new ClaimResult(false, existing);
        }
    }
}

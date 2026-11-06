package com.jqueue.infrastructure.redis;

import java.time.Duration;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.IdempotencyKey;
import com.jqueue.domain.job.vo.JobId;
import com.jqueue.domain.persistence.IdempotencyStore;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/**
 * Implements deduplication on Redis {@code SET NX}.
 *
 * <p>SET NX is the whole mechanism, and it is enough: it is a single-round-trip
 * compare-and-set that Redis executes atomically, so two concurrent enqueues carrying the
 * same key cannot both win. The loser learns the winner's job id from a follow-up GET and
 * returns that job to its caller, which is what makes a retried HTTP request return the
 * original job instead of a duplicate.
 */
public final class RedisIdempotencyStore implements IdempotencyStore {

    private final UnifiedJedis redis;
    private final KeyBuilder keys;

    public RedisIdempotencyStore(UnifiedJedis redis, KeyBuilder keys) {
        this.redis = redis;
        this.keys = keys;
    }

    @Override
    public ClaimResult claim(IdempotencyKey key, JobId jobId, Duration ttl) {
        if (key.isZero()) {
            // No key means the caller opted out of deduplication; treat it as an uncontested
            // claim so the enqueue path has no special case.
            return ClaimResult.won();
        }

        String redisKey = keys.idempotency(key);

        String reply;
        try {
            reply = redis.set(redisKey, jobId.value(), SetParams.setParams().nx().px(ttl.toMillis()));
        } catch (RuntimeException e) {
            throw AppError.internal("idempotency_claim_failed", "failed to claim idempotency key", e);
        }
        if ("OK".equals(reply)) {
            return ClaimResult.won();
        }

        String existing;
        try {
            existing = redis.get(redisKey);
        } catch (RuntimeException e) {
            throw AppError.internal("idempotency_read_failed", "failed to read idempotency claim", e);
        }
        if (existing == null) {
            // The claim expired between SET NX and GET. Report the key as taken rather than
            // retrying: the caller surfaces a conflict, and the next attempt will find the
            // key free. Looping here would turn an expiring key into an unbounded retry.
            return ClaimResult.heldBy(null);
        }
        try {
            return ClaimResult.heldBy(JobId.of(existing));
        } catch (AppError malformed) {
            return ClaimResult.heldBy(null);
        }
    }

    @Override
    public void release(IdempotencyKey key) {
        if (key.isZero()) {
            return;
        }
        try {
            redis.del(keys.idempotency(key));
        } catch (RuntimeException e) {
            throw AppError.internal("idempotency_release_failed", "failed to release idempotency key", e);
        }
    }
}

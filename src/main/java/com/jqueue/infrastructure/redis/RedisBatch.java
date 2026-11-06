package com.jqueue.infrastructure.redis;

import java.util.function.Consumer;

import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.PipeliningBase;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Runs a group of commands in one round trip.
 *
 * <p>On a standalone server the group is a MULTI/EXEC transaction, so it applies all at
 * once. A cluster cannot run a transaction across hash slots, so there the group falls back
 * to a pipeline — still one round trip per node, but not atomic. Every caller of
 * {@link #atomically} is written so that a partial application is safe to repeat.
 */
final class RedisBatch {

    private RedisBatch() {
    }

    /** Runs {@code commands} as a transaction where possible, or a pipeline otherwise. */
    static void atomically(UnifiedJedis jedis, Consumer<PipeliningBase> commands) {
        if (jedis instanceof RedisClusterClient) {
            pipelined(jedis, commands);
            return;
        }
        try (AbstractTransaction tx = jedis.multi()) {
            commands.accept(tx);
            tx.exec();
        }
    }

    /** Runs {@code commands} as a pipeline: one round trip, no atomicity. */
    static void pipelined(UnifiedJedis jedis, Consumer<PipeliningBase> commands) {
        try (AbstractPipeline pipeline = jedis.pipelined()) {
            commands.accept(pipeline);
            pipeline.sync();
        }
    }
}

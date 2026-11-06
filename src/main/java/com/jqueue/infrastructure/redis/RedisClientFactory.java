package com.jqueue.infrastructure.redis;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.jqueue.common.errors.AppError;
import com.jqueue.common.errors.ErrorKind;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Builds the Redis client and verifies it can reach the server.
 *
 * <p>One address yields a pooled standalone client and several yield a cluster client.
 * Both are {@link UnifiedJedis}, so the deployment decides the topology by how many
 * addresses it supplies and no application code changes.
 */
public final class RedisClientFactory {

    /**
     * The connection settings, kept here rather than imported from the configuration
     * package so that this package depends on nothing above it.
     */
    public record ClientConfig(
            List<String> addrs,
            String username,
            String password,
            int db,
            int poolSize,
            int minIdleConns,
            Duration dialTimeout,
            Duration readTimeout,
            Duration writeTimeout) {
    }

    private RedisClientFactory() {
    }

    /** Builds the client, failing fast with an UNAVAILABLE error when Redis is unreachable. */
    public static UnifiedJedis create(ClientConfig cfg) {
        List<String> addrs = cfg.addrs() == null || cfg.addrs().isEmpty() ? List.of("127.0.0.1:6379") : cfg.addrs();

        JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
                .user(blankToNull(cfg.username()))
                .password(blankToNull(cfg.password()))
                .database(cfg.db())
                .connectionTimeoutMillis(millis(cfg.dialTimeout(), 5_000))
                .socketTimeoutMillis(Math.max(millis(cfg.readTimeout(), 3_000), millis(cfg.writeTimeout(), 3_000)))
                .clientName("jqueue")
                .build();

        GenericObjectPoolConfig<Connection> pool = new GenericObjectPoolConfig<>();
        pool.setMaxTotal(cfg.poolSize() > 0 ? cfg.poolSize() : 20);
        pool.setMaxIdle(cfg.poolSize() > 0 ? cfg.poolSize() : 20);
        pool.setMinIdle(Math.max(cfg.minIdleConns(), 0));
        pool.setJmxEnabled(false);

        UnifiedJedis client;
        if (addrs.size() == 1) {
            client = RedisClient.builder()
                    .hostAndPort(HostAndPort.from(addrs.getFirst()))
                    .clientConfig(clientConfig)
                    .poolConfig(pool)
                    .build();
        } else {
            Set<HostAndPort> nodes = new LinkedHashSet<>();
            for (String addr : addrs) {
                nodes.add(HostAndPort.from(addr));
            }
            client = RedisClusterClient.builder()
                    .nodes(nodes)
                    .clientConfig(clientConfig)
                    .poolConfig(pool)
                    .build();
        }

        try {
            client.ping();
        } catch (RuntimeException e) {
            client.close();
            throw AppError.wrap(ErrorKind.UNAVAILABLE, "redis_unavailable", "failed to connect to redis", e);
        }
        return client;
    }

    private static int millis(Duration d, int fallback) {
        return d == null || d.isZero() || d.isNegative() ? fallback : (int) Math.min(d.toMillis(), Integer.MAX_VALUE);
    }

    private static String blankToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}

package com.jqueue.infrastructure.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jqueue.common.errors.AppError;
import com.jqueue.domain.job.vo.QueueName;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.domain.queue.Queue;
import com.jqueue.domain.queue.QueueErrorCodes;
import com.jqueue.domain.queue.QueueStats;
import com.jqueue.domain.queue.Weight;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;

/** Implements the queue persistence port on Redis. */
public final class RedisQueueRepository implements QueueRepository {

    static final String META_WEIGHT = "weight";
    static final String META_PAUSED = "paused";

    private final UnifiedJedis redis;
    private final KeyBuilder keys;

    public RedisQueueRepository(UnifiedJedis redis, KeyBuilder keys) {
        this.redis = redis;
        this.keys = keys;
    }

    /**
     * Records a queue so it is listed before it has held any job.
     *
     * <p>The weight is written unconditionally — it comes from configuration, so a
     * deployment that changes it should take effect. The paused flag is written only if
     * absent, with HSETNX.
     *
     * <p>That asymmetry is deliberate and load-bearing. Every worker calls register on every
     * start, so writing the paused flag unconditionally would resume a queue an operator
     * paused during an incident the moment any pod restarted or scaled up — silently, and at
     * exactly the wrong moment.
     */
    @Override
    public void register(Queue queue) {
        String metaKey = keys.queueMeta(queue.name());
        try {
            RedisBatch.atomically(redis, batch -> {
                batch.sadd(keys.queueRegistry(), queue.name().value());
                batch.hset(metaKey, META_WEIGHT, Integer.toString(queue.weight().value()));
                batch.hsetnx(metaKey, META_PAUSED, flag(queue.isPaused()));
            });
        } catch (RuntimeException e) {
            throw AppError.internal("queue_register_failed", "failed to register queue", e);
        }
    }

    @Override
    public Queue findByName(QueueName name) {
        boolean exists;
        try {
            exists = redis.sismember(keys.queueRegistry(), name.value());
        } catch (RuntimeException e) {
            throw AppError.internal("queue_read_failed", "failed to read queue", e);
        }
        if (!exists) {
            throw AppError.notFound(QueueErrorCodes.QUEUE_NOT_FOUND, "no queue named " + name);
        }
        return loadMeta(name);
    }

    @Override
    public List<Queue> listAll() {
        Set<String> names;
        try {
            names = redis.smembers(keys.queueRegistry());
        } catch (RuntimeException e) {
            throw AppError.internal("queue_list_failed", "failed to list queues", e);
        }
        List<Queue> queues = new ArrayList<>();
        for (String raw : names) {
            QueueName name;
            try {
                name = QueueName.of(raw);
            } catch (AppError ignored) {
                continue;
            }
            queues.add(loadMeta(name));
        }
        return queues;
    }

    @Override
    public void setPaused(QueueName name, boolean paused) {
        try {
            redis.hset(keys.queueMeta(name), META_PAUSED, flag(paused));
        } catch (RuntimeException e) {
            throw AppError.internal("queue_pause_failed", "failed to update queue pause state", e);
        }
    }

    @Override
    public QueueStats stats(QueueName name) {
        return statsFor(loadMeta(name));
    }

    @Override
    public List<QueueStats> statsAll() {
        List<QueueStats> stats = new ArrayList<>();
        for (Queue queue : listAll()) {
            stats.add(statsFor(queue));
        }
        stats.sort(QueueStats.BY_NAME);
        return stats;
    }

    /** Counts every lifecycle set of one queue in a single pipeline. */
    private QueueStats statsFor(Queue queue) {
        QueueName name = queue.name();
        List<Response<Long>> counts = new ArrayList<>(6);
        try {
            RedisBatch.pipelined(redis, batch -> {
                counts.add(batch.zcard(keys.pending(name)));
                counts.add(batch.zcard(keys.active(name)));
                counts.add(batch.zcard(keys.scheduled(name)));
                counts.add(batch.zcard(keys.retrying(name)));
                counts.add(batch.zcard(keys.completed(name)));
                counts.add(batch.zcard(keys.dead(name)));
            });
        } catch (RuntimeException e) {
            throw AppError.internal("queue_stats_failed", "failed to read queue stats", e);
        }
        return new QueueStats(
                name, queue.isPaused(), queue.weight(),
                counts.get(0).get(), counts.get(1).get(), counts.get(2).get(),
                counts.get(3).get(), counts.get(4).get(), counts.get(5).get());
    }

    /**
     * Reads a queue's weight and paused flag, defaulting a queue that has no meta hash yet
     * rather than failing: a queue can be created implicitly by an enqueue, and a missing
     * meta hash simply means nobody has configured it.
     */
    private Queue loadMeta(QueueName name) {
        Map<String, String> meta;
        try {
            meta = redis.hgetAll(keys.queueMeta(name));
        } catch (RuntimeException e) {
            throw AppError.internal("queue_read_failed", "failed to read queue metadata", e);
        }
        Weight weight;
        try {
            weight = Weight.of((int) JobDocumentMapper.parseLong(meta.get(META_WEIGHT), Weight.DEFAULT.value()));
        } catch (AppError invalid) {
            weight = Weight.DEFAULT;
        }
        return Queue.reconstitute(name, weight, "1".equals(meta.get(META_PAUSED)));
    }

    /** Renders a boolean as the "1"/"0" the Lua scripts compare against. */
    private static String flag(boolean value) {
        return value ? "1" : "0";
    }
}

package com.jqueue.infrastructure.redis;

import java.util.List;

import redis.clients.jedis.UnifiedJedis;

/**
 * The Lua scripts that give the broker its atomicity.
 *
 * <p>The scripts live in {@code src/main/resources/lua} as plain {@code .lua} files rather
 * than Java string literals, so they keep syntax highlighting, a reviewer can read the
 * queueing logic without wading through escaping, and the comments explaining each
 * invariant sit next to the code they describe. They are byte-for-byte the same scripts the
 * Go implementation uses.
 */
public final class Scripts {

    /** Writes a job document and inserts it into a lifecycle set. */
    public static final LuaScript ENQUEUE = LuaScript.load("enqueue");
    /** Claims one job for a worker across a weighted queue order. */
    public static final LuaScript DEQUEUE = LuaScript.load("dequeue");
    /** Moves a job between two lifecycle sets. */
    public static final LuaScript TRANSITION = LuaScript.load("transition");
    /** Moves due scheduled or retrying jobs into pending. */
    public static final LuaScript PROMOTE = LuaScript.load("promote");
    /** Returns jobs with lapsed leases to pending. */
    public static final LuaScript RECLAIM = LuaScript.load("reclaim");
    /** Renews an active job's lease. */
    public static final LuaScript EXTEND_LEASE = LuaScript.load("extend_lease");
    /** Trims expired entries from the completed set. */
    public static final LuaScript PURGE_COMPLETED = LuaScript.load("purge_completed");

    private Scripts() {
    }

    /** Every script, for preloading at startup. */
    public static List<LuaScript> all() {
        return List.of(ENQUEUE, DEQUEUE, TRANSITION, PROMOTE, RECLAIM, EXTEND_LEASE, PURGE_COMPLETED);
    }

    /**
     * Pushes every script into the server's script cache so the first job of the process
     * pays the same cost as the millionth.
     *
     * <p>A failure here is not fatal — {@link LuaScript#run} reloads a missing script on
     * demand — so the caller logs and continues.
     */
    public static void preloadAll(UnifiedJedis jedis) {
        for (LuaScript script : all()) {
            script.preload(jedis);
        }
    }
}

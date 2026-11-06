package com.jqueue.infrastructure.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * A Lua script that runs by SHA, reloading itself on demand.
 *
 * <p>The SHA is computed locally, so the common path is a single EVALSHA and the script
 * source crosses the wire only the first time a server sees it (or after a SCRIPT FLUSH or
 * a failover to a replica that never loaded it). A NOSCRIPT reply falls back to EVAL, which
 * also caches the script for every later call.
 */
public final class LuaScript {

    private final String name;
    private final String source;
    private final String sha;

    private LuaScript(String name, String source) {
        this.name = name;
        this.source = source;
        this.sha = sha1(source);
    }

    /** Loads a script from {@code classpath:/lua/<name>.lua}. */
    static LuaScript load(String name) {
        String path = "/lua/" + name + ".lua";
        try (InputStream in = LuaScript.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing lua script on the classpath: " + path);
            }
            return new LuaScript(name, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read lua script " + path, e);
        }
    }

    public String name() {
        return name;
    }

    public String sha() {
        return sha;
    }

    /** Runs the script, falling back to EVAL when the server's script cache is cold. */
    public Object run(UnifiedJedis jedis, List<String> keys, List<String> args) {
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisDataException e) {
            if (e.getMessage() == null || !e.getMessage().startsWith("NOSCRIPT")) {
                throw e;
            }
            return jedis.eval(source, keys, args);
        }
    }

    /** Runs the script and reads an integer reply. */
    public long runForLong(UnifiedJedis jedis, List<String> keys, List<String> args) {
        Object reply = run(jedis, keys, args);
        if (reply instanceof Long value) {
            return value;
        }
        throw new IllegalStateException("lua script " + name + " returned " + reply + " instead of an integer");
    }

    /** Pushes the script into the server's script cache. */
    public void preload(UnifiedJedis jedis) {
        jedis.scriptLoad(source);
    }

    private static String sha1(String source) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is unavailable", e);
        }
    }
}

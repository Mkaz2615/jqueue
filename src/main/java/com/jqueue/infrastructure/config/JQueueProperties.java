package com.jqueue.infrastructure.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.jqueue.application.enqueuer.EnqueuerConfig;
import com.jqueue.application.processor.ProcessorConfig;
import com.jqueue.common.errors.AppError;
import com.jqueue.infrastructure.redis.RedisClientFactory;
import com.jqueue.infrastructure.scheduler.SchedulerConfig;
import com.jqueue.infrastructure.worker.WorkerPoolConfig;

/**
 * The root configuration document, bound from the {@code jqueue:} section.
 *
 * <p>Every field has a working default, so the service runs with no file and no
 * environment at all against a local Redis. {@code config/config.local.yaml} documents the
 * full surface; environment variables prefixed {@code JQUEUE_} override it, and secrets
 * only ever come from the environment.
 *
 * <p>The narrow accessors at the bottom hand each component exactly the slice of
 * configuration it needs, so the worker pool does not depend on the Redis section and a
 * unit test for one component never has to build the whole document. This is the
 * interface segregation principle applied to configuration.
 */
@ConfigurationProperties(prefix = "jqueue")
public class JQueueProperties {

    private String env = "local";
    private final Server server = new Server();
    private final Logger logger = new Logger();
    private final Redis redis = new Redis();
    private final Worker worker = new Worker();
    private final Scheduler scheduler = new Scheduler();
    private final Job job = new Job();

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public Server getServer() {
        return server;
    }

    public Logger getLogger() {
        return logger;
    }

    public Redis getRedis() {
        return redis;
    }

    public Worker getWorker() {
        return worker;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public Job getJob() {
        return job;
    }

    /**
     * Rejects a configuration that cannot produce a working process.
     *
     * <p>This runs at startup so a bad value fails loudly at boot rather than at 3am when the
     * first job of that shape arrives.
     */
    public void validate() {
        if (server.publicPort <= 0 || server.publicPort > 65535) {
            throw AppError.invalid("invalid_config", "server.public_port must be between 1 and 65535");
        }
        if (server.privatePort <= 0 || server.privatePort > 65535) {
            throw AppError.invalid("invalid_config", "server.private_port must be between 1 and 65535");
        }
        if (server.publicPort == server.privatePort) {
            throw AppError.invalid("invalid_config", "server.public_port and server.private_port must differ");
        }
        if (redis.addrs == null || redis.addrs.isEmpty()) {
            throw AppError.invalid("invalid_config", "redis.addrs must not be empty");
        }
        if (worker.enabled && worker.concurrency <= 0) {
            throw AppError.invalid("invalid_config", "worker.concurrency must be greater than 0");
        }
        if (worker.enabled && worker.effectiveQueues().isEmpty()) {
            throw AppError.invalid("invalid_config", "worker.queues must not be empty when the worker is enabled");
        }
    }

    /** Reports whether the process is running in a production-like environment. */
    public boolean isProduction() {
        return switch (env == null ? "" : env.toLowerCase(Locale.ROOT)) {
            case "prod", "production", "stg", "staging" -> true;
            default -> false;
        };
    }

    // ------------------------------------------------------------------------------------
    // Narrow accessors
    // ------------------------------------------------------------------------------------

    public RedisClientFactory.ClientConfig redisClientConfig() {
        return new RedisClientFactory.ClientConfig(
                redis.addrs, redis.username, redis.password, redis.db, redis.poolSize,
                redis.minIdleConns, redis.dialTimeout, redis.readTimeout, redis.writeTimeout);
    }

    public EnqueuerConfig enqueuerConfig() {
        return new EnqueuerConfig(job.idempotencyTtl);
    }

    public ProcessorConfig processorConfig() {
        return new ProcessorConfig(job.completedRetention);
    }

    public WorkerPoolConfig workerPoolConfig() {
        return new WorkerPoolConfig(worker.concurrency, worker.effectiveQueues(), worker.pollInterval,
                worker.shutdownTimeout, worker.leaseDuration);
    }

    public SchedulerConfig schedulerConfig() {
        return new SchedulerConfig(scheduler.promoteInterval, scheduler.reclaimInterval,
                scheduler.janitorInterval, scheduler.metricsInterval, scheduler.batchLimit);
    }

    // ------------------------------------------------------------------------------------
    // Sections
    // ------------------------------------------------------------------------------------

    /**
     * The two HTTP listeners. Public and private are separate ports so a deployment can
     * expose the job API to clients while keeping metrics, health and the operator
     * dashboard on an internal network.
     */
    public static class Server {
        private String name = "jqueue";
        private String version = "1.0.0";
        private int publicPort = 8080;
        private int privatePort = 9090;
        private Duration readTimeout = Duration.ofSeconds(15);
        private Duration writeTimeout = Duration.ofSeconds(30);
        private Duration shutdownTimeout = Duration.ofSeconds(20);

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public int getPublicPort() {
            return publicPort;
        }

        public void setPublicPort(int publicPort) {
            this.publicPort = publicPort;
        }

        public int getPrivatePort() {
            return privatePort;
        }

        public void setPrivatePort(int privatePort) {
            this.privatePort = privatePort;
        }

        public Duration getReadTimeout() {
            return readTimeout;
        }

        public void setReadTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
        }

        public Duration getWriteTimeout() {
            return writeTimeout;
        }

        public void setWriteTimeout(Duration writeTimeout) {
            this.writeTimeout = writeTimeout;
        }

        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }
    }

    /** The structured logger. */
    public static class Logger {
        /** One of debug, info, warn, error. */
        private String level = "info";

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }
    }

    /** The Redis connection. */
    public static class Redis {
        private List<String> addrs = List.of("127.0.0.1:6379");
        private String username = "";
        private String password = "";
        private int db = 0;
        /** Prefixes every key, so several deployments can share one Redis. */
        private String namespace = "jqueue";
        private int poolSize = 20;
        private int minIdleConns = 4;
        private Duration dialTimeout = Duration.ofSeconds(5);
        private Duration readTimeout = Duration.ofSeconds(3);
        private Duration writeTimeout = Duration.ofSeconds(3);

        public List<String> getAddrs() {
            return addrs;
        }

        public void setAddrs(List<String> addrs) {
            this.addrs = addrs;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getDb() {
            return db;
        }

        public void setDb(int db) {
            this.db = db;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }

        public int getMinIdleConns() {
            return minIdleConns;
        }

        public void setMinIdleConns(int minIdleConns) {
            this.minIdleConns = minIdleConns;
        }

        public Duration getDialTimeout() {
            return dialTimeout;
        }

        public void setDialTimeout(Duration dialTimeout) {
            this.dialTimeout = dialTimeout;
        }

        public Duration getReadTimeout() {
            return readTimeout;
        }

        public void setReadTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
        }

        public Duration getWriteTimeout() {
            return writeTimeout;
        }

        public void setWriteTimeout(Duration writeTimeout) {
            this.writeTimeout = writeTimeout;
        }
    }

    /** The worker pool. */
    public static class Worker {
        /** Defaults applied when no queues are configured at all. */
        static final Map<String, Integer> DEFAULT_QUEUES = defaultQueues();

        /** Set false to run an API-only process. */
        private boolean enabled = true;
        private int concurrency = 10;
        /**
         * Queue weights. Deliberately null by default rather than pre-filled: a configured
         * map replaces the defaults instead of being merged into them.
         */
        private Map<String, Integer> queues;
        private Duration pollInterval = Duration.ofMillis(500);
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private Duration leaseDuration = Duration.ofSeconds(60);

        private static Map<String, Integer> defaultQueues() {
            Map<String, Integer> queues = new LinkedHashMap<>();
            queues.put("critical", 6);
            queues.put("default", 3);
            queues.put("low", 1);
            return Map.copyOf(queues);
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        /**
         * The configured map, exactly as bound — possibly null. It deliberately does not fall
         * back to the defaults: Spring's binder merges into whatever a getter returns, so a
         * getter that returned the defaults would mix them into every configured map.
         */
        public Map<String, Integer> getQueues() {
            return queues;
        }

        /** The queues the pool should drain: the configured map, or the defaults when unset. */
        public Map<String, Integer> effectiveQueues() {
            return queues == null || queues.isEmpty() ? DEFAULT_QUEUES : queues;
        }

        public void setQueues(Map<String, Integer> queues) {
            this.queues = queues;
        }

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }

        public Duration getLeaseDuration() {
            return leaseDuration;
        }

        public void setLeaseDuration(Duration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }
    }

    /** The periodic sweeps. */
    public static class Scheduler {
        /** Set false to run a worker-only process with no sweeps. */
        private boolean enabled = true;
        private Duration promoteInterval = Duration.ofSeconds(1);
        private Duration reclaimInterval = Duration.ofSeconds(30);
        private Duration janitorInterval = Duration.ofMinutes(1);
        private Duration metricsInterval = Duration.ofSeconds(5);
        private long batchLimit = 500;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getPromoteInterval() {
            return promoteInterval;
        }

        public void setPromoteInterval(Duration promoteInterval) {
            this.promoteInterval = promoteInterval;
        }

        public Duration getReclaimInterval() {
            return reclaimInterval;
        }

        public void setReclaimInterval(Duration reclaimInterval) {
            this.reclaimInterval = reclaimInterval;
        }

        public Duration getJanitorInterval() {
            return janitorInterval;
        }

        public void setJanitorInterval(Duration janitorInterval) {
            this.janitorInterval = janitorInterval;
        }

        public Duration getMetricsInterval() {
            return metricsInterval;
        }

        public void setMetricsInterval(Duration metricsInterval) {
            this.metricsInterval = metricsInterval;
        }

        public long getBatchLimit() {
            return batchLimit;
        }

        public void setBatchLimit(long batchLimit) {
            this.batchLimit = batchLimit;
        }
    }

    /** Job-level defaults. */
    public static class Job {
        /** How long a completed job stays visible on the dashboard. */
        private Duration completedRetention = Duration.ofHours(1);
        /**
         * How long an idempotency key is held. Must exceed the longest client retry window,
         * or a retried request will slip past deduplication.
         */
        private Duration idempotencyTtl = Duration.ofHours(24);

        public Duration getCompletedRetention() {
            return completedRetention;
        }

        public void setCompletedRetention(Duration completedRetention) {
            this.completedRetention = completedRetention;
        }

        public Duration getIdempotencyTtl() {
            return idempotencyTtl;
        }

        public void setIdempotencyTtl(Duration idempotencyTtl) {
            this.idempotencyTtl = idempotencyTtl;
        }
    }
}

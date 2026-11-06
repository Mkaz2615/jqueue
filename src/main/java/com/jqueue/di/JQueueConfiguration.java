package com.jqueue.di;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.jqueue.application.enqueuer.DefaultEnqueuerService;
import com.jqueue.application.inspector.DefaultInspectorService;
import com.jqueue.application.processor.DefaultHandlerRegistry;
import com.jqueue.application.processor.DefaultJobProcessorService;
import com.jqueue.application.services.EnqueuerService;
import com.jqueue.application.services.HandlerRegistry;
import com.jqueue.application.services.InspectorService;
import com.jqueue.common.errors.AppError;
import com.jqueue.common.uid.IdGenerator;
import com.jqueue.common.uid.RandomIdGenerator;
import com.jqueue.domain.metrics.MetricsRecorder;
import com.jqueue.domain.persistence.IdempotencyStore;
import com.jqueue.domain.persistence.JobBroker;
import com.jqueue.domain.persistence.JobRepository;
import com.jqueue.domain.persistence.QueueRepository;
import com.jqueue.infrastructure.config.JQueueProperties;
import com.jqueue.infrastructure.metrics.MetricsRegistryFactory;
import com.jqueue.infrastructure.metrics.PrometheusMetricsRecorder;
import com.jqueue.infrastructure.redis.KeyBuilder;
import com.jqueue.infrastructure.redis.RedisClientFactory;
import com.jqueue.infrastructure.redis.RedisIdempotencyStore;
import com.jqueue.infrastructure.redis.RedisJobBroker;
import com.jqueue.infrastructure.redis.RedisJobRepository;
import com.jqueue.infrastructure.redis.RedisQueueRepository;
import com.jqueue.infrastructure.redis.Scripts;
import com.jqueue.infrastructure.scheduler.MaintenanceScheduler;
import com.jqueue.infrastructure.web.DashboardPage;
import com.jqueue.infrastructure.worker.WorkerPool;
import com.jqueue.tasks.DemoTasks;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import redis.clients.jedis.UnifiedJedis;

/**
 * The composition root.
 *
 * <p>This is the only class that knows which concrete type satisfies which interface. The
 * domain and application layers carry no framework annotations at all — they are plain
 * Java, constructed here with explicit constructor calls — which is what makes the
 * dependency inversion real rather than decorative: swap Redis for Postgres and this file,
 * plus the new adapter package, is all that changes.
 *
 * <p>Every bean method's return type is the port, not the implementation, so consumers ask
 * for the interface and never have to know, or repeat, which class satisfies it.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JQueueProperties.class)
public class JQueueConfiguration {

    private static final Logger log = LoggerFactory.getLogger(JQueueConfiguration.class);

    // region — shared kernel

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    IdGenerator idGenerator() {
        return new RandomIdGenerator();
    }

    // endregion

    // region — redis infrastructure

    /**
     * The Redis connection, verified at startup and closed after every lifecycle component
     * has stopped.
     */
    @Bean(destroyMethod = "close")
    UnifiedJedis redisClient(JQueueProperties props) {
        props.validate();
        UnifiedJedis client = RedisClientFactory.create(props.redisClientConfig());

        // Preloading is an optimisation, not a requirement: a script missing from the
        // server's cache is reloaded on first use, so a failure here is logged and ignored.
        try {
            Scripts.preloadAll(client);
        } catch (RuntimeException e) {
            log.atWarn()
                    .addKeyValue("error", AppError.describe(e))
                    .log("failed to preload lua scripts; they will load on first use");
        }
        return client;
    }

    @Bean
    KeyBuilder keyBuilder(JQueueProperties props) {
        return new KeyBuilder(props.getRedis().getNamespace());
    }

    @Bean
    JobRepository jobRepository(UnifiedJedis redis, KeyBuilder keys, Clock clock) {
        return new RedisJobRepository(redis, keys, clock);
    }

    @Bean
    JobBroker jobBroker(UnifiedJedis redis, KeyBuilder keys, Clock clock) {
        return new RedisJobBroker(redis, keys, clock);
    }

    @Bean
    QueueRepository queueRepository(UnifiedJedis redis, KeyBuilder keys) {
        return new RedisQueueRepository(redis, keys);
    }

    @Bean
    IdempotencyStore idempotencyStore(UnifiedJedis redis, KeyBuilder keys) {
        return new RedisIdempotencyStore(redis, keys);
    }

    // endregion

    // region — metrics

    @Bean(destroyMethod = "close")
    PrometheusMeterRegistry prometheusMeterRegistry() {
        return MetricsRegistryFactory.create();
    }

    @Bean
    MetricsRecorder metricsRecorder(PrometheusMeterRegistry registry) {
        return new PrometheusMetricsRecorder(registry);
    }

    // endregion

    // region — application services

    /**
     * The task handler registry, shared by the worker pool and the readiness endpoint so
     * both agree on what this process can actually run.
     *
     * <p>{@link DemoTasks#registerAll} is the seam an application customises: replace it
     * with its own registrations and nothing else in the project changes.
     */
    @Bean
    HandlerRegistry handlerRegistry() {
        HandlerRegistry registry = new DefaultHandlerRegistry();
        DemoTasks.registerAll(registry);
        return registry;
    }

    @Bean
    EnqueuerService enqueuerService(
            JobBroker broker, IdempotencyStore idempotency, JobRepository jobs,
            IdGenerator ids, Clock clock, JQueueProperties props) {
        return new DefaultEnqueuerService(broker, idempotency, jobs, ids, clock, props.enqueuerConfig());
    }

    @Bean
    InspectorService inspectorService(
            JobRepository jobs, QueueRepository queues, JobBroker broker, Clock clock) {
        return new DefaultInspectorService(jobs, queues, broker, clock);
    }

    @Bean
    DashboardPage dashboardPage() {
        return new DashboardPage();
    }

    // endregion

    // region — background components

    /** Workers run only when enabled, so the same image can serve as an API-only pod. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBooleanProperty(name = "jqueue.worker.enabled", matchIfMissing = true)
    static class WorkerConfiguration {

        @Bean(destroyMethod = "close")
        DefaultJobProcessorService jobProcessorService(
                HandlerRegistry registry, JobBroker broker, JobRepository jobs,
                MetricsRecorder metrics, Clock clock, JQueueProperties props) {
            return new DefaultJobProcessorService(registry, broker, jobs, metrics, clock, props.processorConfig());
        }

        @Bean
        WorkerPool workerPool(
                JobBroker broker, QueueRepository queues, DefaultJobProcessorService processor,
                MetricsRecorder metrics, IdGenerator ids, JQueueProperties props) {
            return new WorkerPool(broker, queues, processor, metrics, ids, props.workerPoolConfig());
        }

        @Bean
        SmartLifecycle workerPoolLifecycle(WorkerPool pool) {
            return new ManagedComponent("worker pool", ManagedComponent.WORKER_POOL_PHASE, pool::start, pool::stop);
        }
    }

    /**
     * Sweeps run only when enabled. Exactly one deployment in a cluster needs them; running
     * more is safe, just redundant.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBooleanProperty(name = "jqueue.scheduler.enabled", matchIfMissing = true)
    static class SchedulerConfiguration {

        @Bean
        MaintenanceScheduler maintenanceScheduler(
                JobBroker broker, JobRepository jobs, QueueRepository queues,
                MetricsRecorder metrics, Clock clock, JQueueProperties props) {
            return new MaintenanceScheduler(broker, jobs, queues, metrics, clock, props.schedulerConfig());
        }

        @Bean
        SmartLifecycle schedulerLifecycle(MaintenanceScheduler scheduler) {
            return new ManagedComponent("scheduler", ManagedComponent.SCHEDULER_PHASE, scheduler::start, scheduler::stop);
        }
    }

    // endregion
}

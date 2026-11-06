package com.jqueue.infrastructure.config;

import static com.jqueue.testing.ErrorAssertions.assertCode;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/** Binds the properties exactly as Spring does at startup, from the documented key forms. */
class JQueuePropertiesTest {

    static JQueueProperties bind(Map<String, String> source) {
        JQueueProperties props = new JQueueProperties();
        new Binder(new MapConfigurationPropertySource(source))
                .bind("jqueue", Bindable.ofInstance(props));
        return props;
    }

    @Test
    void defaultsRunWithNoConfigurationAtAll() {
        JQueueProperties props = bind(Map.of());
        props.validate();

        assertThat(props.getServer().getPublicPort()).isEqualTo(8080);
        assertThat(props.getServer().getPrivatePort()).isEqualTo(9090);
        assertThat(props.workerPoolConfig().queues()).containsOnlyKeys("critical", "default", "low");
        assertThat(props.workerPoolConfig().pollInterval()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void bindsTheSnakeCaseKeysUsedInTheConfigFile() {
        JQueueProperties props = bind(Map.of(
                "jqueue.server.public_port", "8181",
                "jqueue.redis.min_idle_conns", "7",
                "jqueue.worker.poll_interval", "250ms",
                "jqueue.job.idempotency_ttl", "48h"));

        assertThat(props.getServer().getPublicPort()).isEqualTo(8181);
        assertThat(props.getRedis().getMinIdleConns()).isEqualTo(7);
        assertThat(props.getWorker().getPollInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(props.getJob().getIdempotencyTtl()).isEqualTo(Duration.ofHours(48));
    }

    @Test
    void configuredQueuesReplaceTheDefaultsRatherThanMergingWithThem() {
        JQueueProperties props = bind(Map.of(
                "jqueue.worker.queues.emails", "4",
                "jqueue.worker.queues.reports", "1"));

        // A merge would silently add critical/default/low to every deployment's polling
        // order, draining queues nobody configured this process to drain.
        assertThat(props.workerPoolConfig().queues()).containsOnly(
                Map.entry("emails", 4), Map.entry("reports", 1));
    }

    @Test
    void validationRejectsUnworkableConfiguration() {
        assertCode(() -> bind(Map.of("jqueue.server.private_port", "8080")).validate(), "invalid_config");
        assertCode(() -> bind(Map.of("jqueue.server.public_port", "70000")).validate(), "invalid_config");
        assertCode(() -> bind(Map.of("jqueue.worker.concurrency", "0")).validate(), "invalid_config");

        // An API-only process may have no worker settings at all.
        bind(Map.of("jqueue.worker.enabled", "false", "jqueue.worker.concurrency", "0")).validate();
    }

    @Test
    void recognisesProductionLikeEnvironments() {
        assertThat(bind(Map.of("jqueue.env", "staging")).isProduction()).isTrue();
        assertThat(bind(Map.of("jqueue.env", "local")).isProduction()).isFalse();
    }
}
